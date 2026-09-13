#pragma once

#include <atomic>
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <future>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <queue>
#include <set>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "replica_state.hpp"
#include "../concurrency/incarnation.hpp"
#include "../engine/put_path.hpp"
#include "../engine/worker.hpp"
#include "../index/hash_table.hpp"
#include "../logging/log.hpp"
#include "../network/message.hpp"
#include "../network/endpoint.hpp"
#include "../network/socket_utils.hpp"
#include "../recovery/recovery_coordinator.hpp"
#include "../storage/segment_store.hpp"

struct Request {
    std::string key;
    std::vector<uint8_t> value;
    // The network connection waits on this until the worker knows the final quorm
    std::shared_ptr<std::promise<OperationStatus>> completion;
};

// Batches are bounded independently by entry count and serialized byte size.
constexpr size_t kReplicationBatchEntryLimit = kMaxWireBatchEntries;
constexpr size_t kReplicationBatchByteLimit = 256U * 1024U;
constexpr size_t kReplicationOutstandingBatchWindow = 4;
constexpr size_t kRequestQueueLimit = 1024;
constexpr uint64_t kMaxReplicationWorkers = 64;

// Persistent transport and cumulative progress for one follower. A session is
// owned by one channel thread, so its socket and prefix need no internal lock.
struct ReplicationSession {
    explicit ReplicationSession(PeerEndpoint target)
        : endpoint(std::move(target)) {}
    ~ReplicationSession() { disconnect(); }

    ReplicationSession(const ReplicationSession&) = delete;
    ReplicationSession& operator=(const ReplicationSession&) = delete;

    PeerEndpoint endpoint;              // Follower represented by this channel.
    int socket{-1};                     // -1 means disconnected.
    uint64_t highest_acked{0};          // Cumulative contiguous follower ACK.
    bool prefix_synchronized{false};    // Prefix query completed on this socket.

    void disconnect() {
        if (socket < 0) return;
        DISKEYV_DEBUG("REPLICATION",
                      "follower=" << endpoint.identity()
                                  << " state=disconnect prefix="
                                  << highest_acked);
        ::shutdown(socket, SHUT_RDWR);
        close(socket);
        socket = -1;
        prefix_synchronized = false;
    }

    bool ensure_connected() {
        if (socket >= 0) return true;
        socket = connect_to(endpoint.host, endpoint.port);
        if (socket >= 0) {
            DISKEYV_INFO("REPLICATION",
                         "follower=" << endpoint.identity()
                                     << " state=connected");
        } else {
            DISKEYV_DEBUG("REPLICATION",
                          "follower=" << endpoint.identity()
                                      << " state=connect-failed");
        }
        return socket >= 0;
    }

    using BatchProvider =
        std::function<NetMessage(size_t first_index, size_t end_index)>;

    // Advance this follower through target_sequence. Up to four batches are
    // pipelined, while ACKs are consumed in send order and remain cumulative.
    bool replicate_to(uint64_t term, uint64_t worker_id,
                      size_t target_sequence,
                      const BatchProvider& batch_provider) {
        if (target_sequence == 0) return true;
        int transport_failures = 0;
        while (transport_failures < 2) {
            if (!ensure_connected() ||
                 (!prefix_synchronized &&
                 !synchronize_prefix(term, worker_id, target_sequence))) {
                disconnect();
                ++transport_failures;
                continue;
            }
            if (highest_acked > target_sequence) {
                disconnect();
                return false;
            }
            if (highest_acked == target_sequence) return true;

            bool retry = false;
            while (highest_acked < target_sequence) {
                // Each expected value is the last sequence of one in-flight
                // batch. Matching it prevents a partial ACK from being treated
                // as completion of the entire batch.
                std::vector<uint64_t> expected_acks;
                expected_acks.reserve(kReplicationOutstandingBatchWindow);
                size_t next_index = static_cast<size_t>(highest_acked);
                while (next_index < target_sequence &&
                       expected_acks.size() <
                           kReplicationOutstandingBatchWindow) {
                    const NetMessage batch =
                        batch_provider(next_index, target_sequence);
                    if (batch.type != MsgType::PUT_REPL_BATCH ||
                        batch.term != term || batch.worker_id != worker_id ||
                        batch.entries.empty() ||
                        batch.seq != next_index + 1) {
                        return false;
                    }
                    const uint64_t expected =
                        batch.entries.back().sequence;
                    if (expected <= next_index ||
                        expected > target_sequence ||
                        !send_message(socket, batch)) {
                        retry = true;
                        break;
                    }
                    expected_acks.push_back(expected);
                    next_index = static_cast<size_t>(expected);
                }
                if (retry) break;

                for (uint64_t expected : expected_acks) {
                    NetMessage reply;
                    if (!recv_message(socket, reply) ||
                        reply.type != MsgType::ACK || reply.term != term ||
                        reply.worker_id != worker_id ||
                        reply.seq < highest_acked ||
                        reply.seq > target_sequence) {
                        retry = true;
                        break;
                    }
                    highest_acked = reply.seq;
                    if (reply.status != OperationStatus::OK ||
                        highest_acked != expected) {
                        retry = true;
                        break;
                    }
                }
                if (retry) break;
            }
            if (!retry && highest_acked == target_sequence) return true;
            disconnect();
            ++transport_failures;
        }
        return false;
    }

private:
    // Reconnects do not assume the follower retained the last local ACK; ask
    // for its current in-memory prefix before sending missing history.
    bool synchronize_prefix(uint64_t term, uint64_t worker_id,
                            size_t history_size) {
        NetMessage query;
        query.type = MsgType::PREFIX_QUERY;
        query.term = term;
        query.worker_id = worker_id;

        NetMessage reply;
        if (!send_message(socket, query) || !recv_message(socket, reply) ||
            reply.type != MsgType::PREFIX_REPLY ||
            reply.status != OperationStatus::OK || reply.term != term ||
            reply.worker_id != worker_id || reply.seq > history_size) {
            return false;
        }
        highest_acked = reply.seq;
        prefix_synchronized = true;
        DISKEYV_DEBUG("REPLICATION",
                      "follower=" << endpoint.identity()
                                  << " worker=" << worker_id
                                  << " prefix-synchronized=" << highest_acked
                                  << " target=" << history_size);
        return true;
    }

};

class ParallelReplicationGroup {
public:
    struct Ticket {
        uint64_t generation{0};      // Publication batch observed by channels.
        uint64_t target_sequence{0}; // Required prefix for this client batch.
    };

    ParallelReplicationGroup(uint64_t term, uint64_t worker_id,
                             const std::vector<PeerEndpoint>& endpoints)
        : term_(term), worker_id_(worker_id) {
        channels_.reserve(endpoints.size());
        for (const PeerEndpoint& endpoint : endpoints) {
            channels_.push_back(std::make_unique<Channel>(endpoint));
        }
        try {
            for (size_t index = 0; index < channels_.size(); ++index) {
                channels_[index]->thread =
                    std::thread([this, index] { channel_loop(index); });
            }
        } catch (...) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                stopping_ = true;
            }
            work_cv_.notify_all();
            for (const auto& channel : channels_) {
                if (channel->thread.joinable()) channel->thread.join();
            }
            throw;
        }
        DISKEYV_DEBUG("REPLICATION",
                      "worker=" << worker_id_ << " term=" << term_
                                << " channels=" << channels_.size()
                                << " batch-window="
                                << kReplicationOutstandingBatchWindow);
    }

    ~ParallelReplicationGroup() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
        }
        work_cv_.notify_all();
        for (const auto& channel : channels_) {
            if (channel->thread.joinable()) channel->thread.join();
        }
    }

    ParallelReplicationGroup(const ParallelReplicationGroup&) = delete;
    ParallelReplicationGroup& operator=(const ParallelReplicationGroup&) =
        delete;

    // Copy immutable SegmentStore objects into replayable wire history before
    // waking follower channels. History indices correspond to sequence - 1.
    Ticket publish(const std::vector<ApplyRecord>& records,
                   const SegmentStore& store) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const ApplyRecord& record : records) {
            const ObjectEntry& object =
                store.segments[record.seg_idx]->entries[record.obj_idx];
            if (object.term_id != term_ ||
                object.seq_num != history_.size() + 1) {
                throw std::runtime_error(
                    "worker replication history is not contiguous");
            }
            ReplicationEntry entry;
            entry.sequence = object.seq_num;
            entry.incarnation = object.incarnation;
            entry.segment_index = record.seg_idx;
            entry.object_index = record.obj_idx;
            entry.key = std::string(object.key);
            entry.value = object.value;
            history_.push_back(std::move(entry));
        }
        ++generation_;
        const Ticket ticket{generation_,
                            static_cast<uint64_t>(history_.size())};
        work_cv_.notify_all();
        return ticket;
    }

    // Return as soon as enough replicas cover the ticket, or once every
    // follower has completed an attempt. The returned order statistic is the
    // largest sequence that this quorum is known to contain.
    uint64_t wait_for_commit(const Ticket& ticket, size_t quorum) {
        std::unique_lock<std::mutex> lock(mutex_);
        progress_cv_.wait(lock, [&] {
            size_t at_target = 1;
            size_t completed = 0;
            for (const auto& channel : channels_) {
                if (channel->prefix >= ticket.target_sequence) ++at_target;
                if (channel->completed_generation >= ticket.generation) {
                    ++completed;
                }
            }
            return at_target >= quorum || completed == channels_.size();
        });

        std::vector<uint64_t> prefixes;
        prefixes.reserve(channels_.size() + 1);
        prefixes.push_back(ticket.target_sequence);
        for (const auto& channel : channels_) {
            prefixes.push_back(channel->prefix);
        }
        std::sort(prefixes.begin(), prefixes.end(), std::greater<uint64_t>());
        const uint64_t watermark = prefixes[quorum - 1];
        lock.unlock();
        if (watermark < ticket.target_sequence) {
            DISKEYV_WARN("QUORUM",
                         "worker=" << worker_id_ << " term=" << term_
                                   << " target=" << ticket.target_sequence
                                   << " committed=" << watermark
                                   << " required=" << quorum);
        } else if (ticket.target_sequence == 1 ||
                   ticket.target_sequence % 1024 == 0) {
            DISKEYV_DEBUG("QUORUM",
                          "worker=" << worker_id_ << " term=" << term_
                                    << " committed=" << watermark
                                    << " required=" << quorum);
        }
        return watermark;
    }

private:
    struct Channel {
        explicit Channel(const PeerEndpoint& endpoint) : session(endpoint) {}

        ReplicationSession session;
        std::thread thread;
        uint64_t prefix{0};               // Last ACK copied from the session.
        uint64_t completed_generation{0}; // Last publication attempt finished.
        bool failure_active{false};       // Suppresses repeated outage warnings.
    };

    uint64_t term_;
    uint64_t worker_id_;
    std::mutex mutex_;
    std::condition_variable work_cv_;
    std::condition_variable progress_cv_;
    bool stopping_{false};                  // Requests channel thread exit.
    uint64_t generation_{0};                // Increments once per published batch.
    std::vector<ReplicationEntry> history_; // Immutable replay source per worker.
    std::vector<std::unique_ptr<Channel>> channels_;


    NetMessage make_batch(size_t first_index, size_t end_index) {
        std::lock_guard<std::mutex> lock(mutex_);
        NetMessage batch;
        batch.type = MsgType::PUT_REPL_BATCH;
        batch.term = term_;
        batch.worker_id = worker_id_;
        if (first_index >= end_index || end_index > history_.size()) {
            return batch;
        }
        batch.seq = history_[first_index].sequence;
        size_t bytes = 0;
        for (size_t index = first_index;
             index < end_index &&
             batch.entries.size() < kReplicationBatchEntryLimit;
             ++index) {
            const ReplicationEntry& entry = history_[index];
            const size_t entry_bytes =
                entry.key.size() + entry.value.size() + 6U * sizeof(uint64_t);
            if (!batch.entries.empty() &&
                bytes + entry_bytes > kReplicationBatchByteLimit) {
                break;
            }
            batch.entries.push_back(entry);
            bytes += entry_bytes;
        }
        return batch;
    }

    // Each follower advances independently. Failed channels retry on the
    // 250 ms wake-up so a slow follower cannot delay an already reached quorum.
    void channel_loop(size_t channel_index) {
        Channel& channel = *channels_[channel_index];
        while (true) {
            uint64_t generation = 0;
            size_t target_sequence = 0;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                work_cv_.wait_for(lock, std::chrono::milliseconds(250), [&] {
                    return stopping_ || generation_ > channel.completed_generation;
                });
                if (stopping_) return;
                if (generation_ == 0 ||
                    channel.prefix >= history_.size()) {
                    continue;
                }
                generation = generation_;
                target_sequence = history_.size();
            }

            bool replicated = false;
            try {
                replicated = channel.session.replicate_to(
                    term_, worker_id_, target_sequence,
                    [this](size_t first, size_t last) {
                        return make_batch(first, last);
                    });
            } catch (...) {
                channel.session.disconnect();
            }

            bool report_failure = false;
            bool report_reconnected = false;
            uint64_t prior_prefix = 0;
            uint64_t current_prefix = 0;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                prior_prefix = channel.prefix;
                channel.prefix = channel.session.highest_acked;
                current_prefix = channel.prefix;
                channel.completed_generation = generation;
                report_failure = !replicated && !channel.failure_active;
                report_reconnected = replicated && channel.failure_active;
                channel.failure_active = !replicated;
            }
            if (report_failure) {
                DISKEYV_WARN("REPLICATION",
                             "worker=" << worker_id_ << " term=" << term_
                                       << " follower="
                                       << channel.session.endpoint.identity()
                                       << " state=unavailable target="
                                       << target_sequence << " prefix="
                                       << current_prefix);
            } else if (report_reconnected) {
                DISKEYV_INFO("REPLICATION",
                             "worker=" << worker_id_ << " term=" << term_
                                       << " follower="
                                       << channel.session.endpoint.identity()
                                       << " state=caught-up from="
                                       << prior_prefix << " to="
                                       << current_prefix);
            }
            progress_cv_.notify_all();
        }
    }
};

struct FollowerProgress {
    std::mutex mutex;
    uint64_t term{0}; // Term to which the cumulative prefix belongs.
    uint64_t highest_contiguous_sequence{0}; // Gaps stop advancement here.
};

struct Replica;

struct ConnectionContext {
    ConnectionContext(Replica* owner, int accepted_socket)
        : replica(owner), socket(accepted_socket) {}

    Replica* replica;
    std::atomic<int> socket;         // -1 after connection thread closes it.
    std::atomic<bool> finished{false}; // Allows accept loop to reap the thread.
    pthread_t thread{};
};

struct Replica {
    ReplicaState rs;       // Identity, role, and current epoch.
    SegmentStore store;    // Immutable record storage.
    HashTable ht;          // Latest visible offset for each key.
    IncarnationTable inc;  // Monotonic per-key version source.

    std::vector<pthread_t> workers;
    std::queue<Request> req_q; // Bounded producer/consumer queue for PUTs.
    pthread_mutex_t req_mtx = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t req_cv = PTHREAD_COND_INITIALIZER;
    pthread_cond_t req_space_cv = PTHREAD_COND_INITIALIZER;

    std::atomic<bool> stop{false};            // Shared thread termination flag.
    std::atomic<bool> recovering{false};      // Rejects clients until rebuild ends.
    std::atomic<bool> shutdown_called{false}; // Makes shutdown idempotent.
    std::atomic<int> server_sock{-1};         // Listening socket, or -1 if closed.
    std::vector<PeerEndpoint> peer_endpoints; // Followers used by a leader.
    size_t quorum{1};                         // Majority including this replica.
    pthread_t net_thread{};
    std::mutex connections_mutex;
    std::vector<std::unique_ptr<ConnectionContext>> connections;
    std::atomic<uint64_t> accepted_connections{0}; // Diagnostic counters below.
    std::atomic<uint64_t> received_batches{0};
    std::atomic<uint64_t> largest_received_batch{0};

    std::mutex follower_progress_mutex;
    // Recovery takes this lock exclusively to prevent old-term replication
    // from interleaving with term fencing, installation, or summary creation.
    std::shared_mutex replication_epoch_mutex;
    uint64_t recovery_leader_id{0}; // Candidate authorized in the current term.
    std::unordered_map<uint64_t, std::shared_ptr<FollowerProgress>>
        follower_progress;

    Replica(uint64_t rid, int nworkers, Role role, int port,
            const std::vector<PeerEndpoint>& peers,
            size_t segment_count = 32, uint64_t segment_capacity = 1024,
            size_t index_capacity = 4096,
            bool recover_from_followers = false)
        : rs(rid, role),
          store(segment_count, segment_capacity),
          ht(index_capacity),
          inc(2048),
          peer_endpoints(peers) {
        if (nworkers <= 0 ||
            static_cast<uint64_t>(nworkers) > kMaxReplicationWorkers) {
            throw std::invalid_argument(
                "a replica needs between 1 and 64 workers");
        }
        std::unordered_set<std::string> unique_peers;
        for (const PeerEndpoint& peer : peer_endpoints) {
            if (!unique_peers.insert(peer.identity()).second) {
                throw std::invalid_argument("peer endpoints must be unique");
            }
        }
        if (role == Role::LEADER) {
            quorum = (peer_endpoints.size() + 1) / 2 + 1;
        }
        DISKEYV_INFO("REPLICA",
                     "replica=" << rid << " phase=initializing role="
                                << (role == Role::LEADER ? "leader" : "follower")
                                << " workers=" << nworkers << " peers="
                                << peer_endpoints.size() << " quorum=" << quorum
                                << " recover="
                                << (recover_from_followers ? "yes" : "no"));

        if (recover_from_followers) {
            if (role != Role::LEADER) {
                throw std::invalid_argument(
                    "only a leader can run data consolidation");
            }
            recovering.store(true, std::memory_order_release);
            RecoveryCoordinator coordinator(
                rs, store, ht, peer_endpoints,
                static_cast<size_t>(nworkers));
            coordinator.recover();
            recovering.store(false, std::memory_order_release);
        }

        server_sock.store(create_server(port), std::memory_order_release);
        if (server_sock.load(std::memory_order_acquire) < 0) {
            throw std::runtime_error("failed to create replica server socket");
        }

        start_workers(nworkers);
        start_network_thread();
        DISKEYV_INFO("REPLICA",
                     "replica=" << rid << " phase=ready role="
                                << (role == Role::LEADER ? "leader" : "follower")
                                << " port=" << listening_port() << " term="
                                << rs.current_term.load(std::memory_order_acquire));
        std::cout << "[Replica " << rid << "] listening on port " << port
                  << " as " << (role == Role::LEADER ? "leader" : "follower")
                  << " (quorum " << quorum << ")\n";
    }

    ~Replica() { shutdown(); }

    Replica(const Replica&) = delete;
    Replica& operator=(const Replica&) = delete;

    static void* net_entry(void* arg) {
        static_cast<Replica*>(arg)->network_loop();
        return nullptr;
    }

    static void* connection_entry(void* arg) {
        auto* context = static_cast<ConnectionContext*>(arg);
        const int socket = context->socket.load(std::memory_order_acquire);
        context->replica->connection_loop(socket);
        const int owned_socket = context->socket.exchange(-1, std::memory_order_acq_rel);
        if (owned_socket >= 0) close(owned_socket);
        context->finished.store(true, std::memory_order_release);
        return nullptr;
    }

    void start_network_thread() {
        if (pthread_create(&net_thread, nullptr, net_entry, this) != 0) {
            DISKEYV_ERROR("REPLICA",
                          "replica=" << rs.replica_id
                                     << " failed-to-start=network-thread");
            stop.store(true, std::memory_order_release);
            pthread_cond_broadcast(&req_cv);
            for (pthread_t thread : workers) pthread_join(thread, nullptr);
            const int listening_socket =
                server_sock.exchange(-1, std::memory_order_acq_rel);
            if (listening_socket >= 0) close(listening_socket);
            throw std::runtime_error("failed to start network thread");
        }
    }

    // Accept connections continuously and hand each one to a joinable pthread.
    // Finished contexts are reclaimed by later accepts or final shutdown.
    void network_loop() {
        while (!stop.load(std::memory_order_acquire)) {
            sockaddr_in client_addr{};
            socklen_t len = sizeof(client_addr);
            const int listening_socket = server_sock.load(std::memory_order_acquire);
            if (listening_socket < 0) return;
            const int client = accept(
                listening_socket, reinterpret_cast<sockaddr*>(&client_addr), &len);
            if (client < 0) {
                if (stop.load(std::memory_order_acquire)) return;
                continue;
            }
            reap_finished_connections();
            accepted_connections.fetch_add(1, std::memory_order_relaxed);
            auto context = std::make_unique<ConnectionContext>(this, client);
            ConnectionContext* raw_context = context.get();
            std::lock_guard<std::mutex> lock(connections_mutex);
            connections.push_back(std::move(context));
            if (pthread_create(&raw_context->thread, nullptr, connection_entry, raw_context) != 0) {
                connections.pop_back();
                close(client);
            }
        }
    }

    void reap_finished_connections() {
        std::lock_guard<std::mutex> lock(connections_mutex);
        auto iterator = connections.begin();
        while (iterator != connections.end()) {
            if (!(*iterator)->finished.load(std::memory_order_acquire)) {
                ++iterator;
                continue;
            }
            pthread_join((*iterator)->thread, nullptr);
            iterator = connections.erase(iterator);
        }
    }

    void connection_loop(int sock) {
        while (!stop.load(std::memory_order_acquire)) {
            NetMessage message;
            if (!recv_message(sock, message)) return;
            dispatch_message(sock, message);
        }
    }

    void handle_connection(int sock) {
        NetMessage msg;
        if (!recv_message(sock, msg)) {
            close(sock);
            return;
        }

        dispatch_message(sock, msg);
        close(sock);
    }

    // Central protocol demultiplexer; handlers own request-specific validation
    // and always return the corresponding reply type on the same connection.
    void dispatch_message(int sock, const NetMessage& msg) {
        switch (msg.type) {
            case MsgType::CLIENT_PUT:
                handle_client_put(sock, msg);
                break;
            case MsgType::CLIENT_GET:
                handle_client_get(sock, msg);
                break;
            case MsgType::PUT_REPL:
                handle_replication(sock, msg);
                break;
            case MsgType::PUT_REPL_BATCH:
                handle_replication_batch(sock, msg);
                break;
            case MsgType::PREFIX_QUERY:
                handle_prefix_query(sock, msg);
                break;
            case MsgType::RECOVERY_SUMMARY_REQUEST:
                handle_recovery_summary(sock, msg);
                break;
            case MsgType::RECOVERY_FETCH_REQUEST:
                handle_recovery_fetch(sock, msg);
                break;
            case MsgType::RECOVERY_INSTALL_BATCH:
                handle_recovery_install(sock, msg);
                break;
            case MsgType::TERM_ADVANCE:
                handle_term_advance(sock, msg);
                break;
            case MsgType::CLIENT_HEALTH: {
                NetMessage reply;
                reply.type = MsgType::CLIENT_HEALTH_REPLY;
                reply.status =
                    stop.load(std::memory_order_acquire)
                        ? OperationStatus::SHUTTING_DOWN
                        : recovering.load(std::memory_order_acquire)
                              ? OperationStatus::RECOVERING
                              : OperationStatus::OK;
                send_message(sock, reply);
                break;
            }
            default: {
                DISKEYV_WARN("NETWORK",
                             "replica=" << rs.replica_id
                                        << " invalid-message-type="
                                        << static_cast<unsigned>(msg.type));
                NetMessage reply;
                reply.type = MsgType::ACK;
                reply.status = OperationStatus::INVALID_REQUEST;
                send_message(sock, reply);
                break;
            }
        }
    }

    void handle_client_put(int sock, const NetMessage& msg) {
        NetMessage reply;
        reply.type = MsgType::CLIENT_PUT_REPLY;
        if (recovering.load(std::memory_order_acquire)) {
            reply.status = OperationStatus::RECOVERING;
        } else if (rs.role.load(std::memory_order_acquire) != Role::LEADER) {
            reply.status = OperationStatus::NOT_LEADER;
        } else {
            reply.status = submit_put(msg.key, msg.value);
        }
        if (reply.status != OperationStatus::OK) {
            DISKEYV_WARN("CLIENT",
                         "replica=" << rs.replica_id
                                    << " operation=put key-bytes="
                                    << msg.key.size() << " status="
                                    << status_name(reply.status));
        }
        send_message(sock, reply);
    }

    void handle_client_get(int sock, const NetMessage& msg) {
        NetMessage reply;
        reply.type = MsgType::CLIENT_GET_REPLY;
        if (recovering.load(std::memory_order_acquire)) {
            reply.status = OperationStatus::RECOVERING;
            send_message(sock, reply);
            return;
        }
        ObjectEntry* entry = get(msg.key);
        if (entry == nullptr) {
            reply.status = OperationStatus::NOT_FOUND;
        } else {
            reply.status = OperationStatus::OK;
            reply.term = entry->term_id;
            reply.seq = entry->seq_num;
            reply.incarnation = entry->incarnation;
            reply.value = entry->value;
        }
        send_message(sock, reply);
    }

    void handle_replication(int sock, const NetMessage& msg) {
        std::shared_lock<std::shared_mutex> epoch_lock(
            replication_epoch_mutex);
        NetMessage reply;
        reply.type = MsgType::ACK;
        reply.term = msg.term;
        reply.worker_id = msg.worker_id;

        const bool valid_request =
            rs.role.load(std::memory_order_acquire) == Role::FOLLOWER &&
            msg.term == rs.current_term.load(std::memory_order_acquire) &&
            !msg.key.empty() && msg.key.size() <= ObjectEntry::kMaxKeySize &&
            msg.seq > 0 && msg.worker_id < kMaxReplicationWorkers;
        if (!valid_request) {
            DISKEYV_WARN("FOLLOWER",
                         "replica=" << rs.replica_id
                                    << " rejected=replication-record term="
                                    << msg.term << " worker=" << msg.worker_id
                                    << " sequence=" << msg.seq);
            reply.status = OperationStatus::INVALID_REQUEST;
            send_message(sock, reply);
            return;
        }

        const auto progress = progress_for(msg.worker_id);
        std::lock_guard<std::mutex> lock(progress->mutex);
        reply.status = apply_replication_record(msg, *progress);
        reply.seq = progress->highest_contiguous_sequence;
        if (reply.status != OperationStatus::OK) {
            DISKEYV_WARN("FOLLOWER",
                         "replica=" << rs.replica_id
                                    << " apply=replication-record worker="
                                    << msg.worker_id << " sequence=" << msg.seq
                                    << " prefix=" << reply.seq << " status="
                                    << status_name(reply.status));
        }
        send_message(sock, reply);
    }

    // Apply a batch under one worker-progress lock. The ACK reports the last
    // contiguous sequence, even when a later entry in the batch is rejected.
    void handle_replication_batch(int sock, const NetMessage& msg) {
        std::shared_lock<std::shared_mutex> epoch_lock(
            replication_epoch_mutex);
        NetMessage reply;
        reply.type = MsgType::ACK;
        reply.term = msg.term;
        reply.worker_id = msg.worker_id;

        const bool valid_request =
            rs.role.load(std::memory_order_acquire) == Role::FOLLOWER &&
            msg.term == rs.current_term.load(std::memory_order_acquire) &&
            msg.worker_id < kMaxReplicationWorkers && !msg.entries.empty() &&
            msg.entries.size() <= kReplicationBatchEntryLimit &&
            msg.entries.front().sequence == msg.seq;
        if (!valid_request) {
            DISKEYV_WARN("FOLLOWER",
                         "replica=" << rs.replica_id
                                    << " rejected=replication-batch term="
                                    << msg.term << " worker=" << msg.worker_id
                                    << " sequence=" << msg.seq << " entries="
                                    << msg.entries.size());
            reply.status = OperationStatus::INVALID_REQUEST;
            send_message(sock, reply);
            return;
        }

        received_batches.fetch_add(1, std::memory_order_relaxed);
        uint64_t largest = largest_received_batch.load(std::memory_order_relaxed);
        const uint64_t current_size = static_cast<uint64_t>(msg.entries.size());
        while (largest < current_size &&
               !largest_received_batch.compare_exchange_weak(
                   largest, current_size, std::memory_order_relaxed,
                   std::memory_order_relaxed)) {}

        const auto progress = progress_for(msg.worker_id);
        std::lock_guard<std::mutex> lock(progress->mutex);
        reply.status = OperationStatus::OK;
        // Wire validation checks this too, but the handler preserves the
        // follower invariant even if it is called directly by a test/tool.
        uint64_t expected = msg.seq;
        for (const ReplicationEntry& entry : msg.entries) {
            if (entry.sequence != expected) {
                reply.status = OperationStatus::INVALID_REQUEST;
                break;
            }
            NetMessage record;
            record.type = MsgType::PUT_REPL;
            record.term = msg.term;
            record.seq = entry.sequence;
            record.incarnation = entry.incarnation;
            record.worker_id = msg.worker_id;
            record.segment_index = entry.segment_index;
            record.object_index = entry.object_index;
            record.key = entry.key;
            record.value = entry.value;
            reply.status = apply_replication_record(record, *progress);
            if (reply.status != OperationStatus::OK) break;
            ++expected;
        }
        reply.seq = progress->highest_contiguous_sequence;
        if (reply.status != OperationStatus::OK) {
            DISKEYV_WARN("FOLLOWER",
                         "replica=" << rs.replica_id
                                    << " apply=replication-batch worker="
                                    << msg.worker_id << " first=" << msg.seq
                                    << " entries=" << msg.entries.size()
                                    << " prefix=" << reply.seq << " status="
                                    << status_name(reply.status));
        } else if (reply.seq == 1 || reply.seq % 1024 == 0) {
            DISKEYV_DEBUG("FOLLOWER",
                          "replica=" << rs.replica_id
                                     << " applied-batch worker="
                                     << msg.worker_id << " entries="
                                     << msg.entries.size() << " prefix="
                                     << reply.seq);
        }
        send_message(sock, reply);
    }

    void handle_prefix_query(int sock, const NetMessage& msg) {
        std::shared_lock<std::shared_mutex> epoch_lock(
            replication_epoch_mutex
        );
        NetMessage reply;
        reply.type = MsgType::PREFIX_REPLY;
        reply.term = msg.term;
        reply.worker_id = msg.worker_id;
        if (rs.role.load(std::memory_order_acquire) != Role::FOLLOWER ||
            msg.term != rs.current_term.load(std::memory_order_acquire) ||
            msg.worker_id >= kMaxReplicationWorkers) {
            DISKEYV_WARN("FOLLOWER",
                         "replica=" << rs.replica_id
                                    << " rejected=prefix-query term="
                                    << msg.term << " worker=" << msg.worker_id);
            reply.status = OperationStatus::INVALID_REQUEST;
            send_message(sock, reply);
            return;
        }

        const auto progress = progress_for(msg.worker_id);
        std::lock_guard<std::mutex> lock(progress->mutex);
        reply.seq = progress->term == msg.term
                        ? progress->highest_contiguous_sequence
                        : 0;
        reply.status = progress->term <= msg.term
                           ? OperationStatus::OK
                           : OperationStatus::INVALID_REQUEST;
        send_message(sock, reply);
    }

    void handle_recovery_summary(int sock, const NetMessage& msg) {
        std::unique_lock<std::shared_mutex> epoch_lock(
            replication_epoch_mutex);
        NetMessage reply;
        reply.type = MsgType::RECOVERY_SUMMARY_REPLY;
        reply.term = rs.current_term.load(std::memory_order_acquire);
        const bool authorized = rs.role.load(std::memory_order_acquire) == Role::FOLLOWER &&
            (msg.term == 0 ||
             (msg.term == reply.term &&
              msg.segment_index == recovery_leader_id));
        if (!authorized) {
            DISKEYV_WARN("RECOVERY",
                         "replica=" << rs.replica_id
                                    << " rejected=summary-request term="
                                    << msg.term << " candidate="
                                    << msg.segment_index);
            reply.status = OperationStatus::INVALID_REQUEST;
        } else {
            reply.worker_progress = reconstruct_worker_progress_locked();
            reply.status = OperationStatus::OK;
        }
        send_message(sock, reply);
    }

    // Scan physical storage from a flat cursor and return only this worker's
    // records at or below the requested <term, sequence> recovery prefix.
    void handle_recovery_fetch(int sock, const NetMessage& msg) {
        std::shared_lock<std::shared_mutex> epoch_lock(
            replication_epoch_mutex);
        NetMessage reply;
        reply.type = MsgType::RECOVERY_FETCH_REPLY;
        reply.term = rs.current_term.load(std::memory_order_acquire);
        reply.worker_id = msg.worker_id;
        const bool authorized = rs.role.load(std::memory_order_acquire) == Role::FOLLOWER &&
            msg.term == reply.term &&
            msg.segment_index == recovery_leader_id &&
            msg.worker_id < kMaxReplicationWorkers &&
            msg.incarnation > 0 && msg.object_index > 0;
        if (!authorized || store.segments.empty()) {
            DISKEYV_WARN("RECOVERY",
                         "replica=" << rs.replica_id
                                    << " rejected=fetch-request term="
                                    << msg.term << " worker=" << msg.worker_id
                                    << " candidate=" << msg.segment_index);
            reply.status = OperationStatus::INVALID_REQUEST;
            send_message(sock, reply);
            return;
        }

        const uint64_t capacity = store.segments.front()->capacity;
        if (capacity == 0 ||
            store.segments.size() >
                std::numeric_limits<uint64_t>::max() / capacity) {
            reply.status = OperationStatus::STORAGE_ERROR;
            send_message(sock, reply);
            return;
        }

        const uint64_t total =
            static_cast<uint64_t>(store.segments.size()) * capacity;
        uint64_t cursor = msg.seq;
        if (cursor > total) {
            reply.status = OperationStatus::INVALID_REQUEST;
            send_message(sock, reply);
            return;
        }

        size_t reply_bytes = 0;
        while (cursor < total && reply.entries.size() < kMaxWireRecoveryBatchEntries) {
            const uint64_t flat_index = cursor++;
            const size_t segment_index = static_cast<size_t>(flat_index / capacity);
            const uint64_t object_index = flat_index % capacity;
            const Segment& segment = *store.segments[segment_index];
            if (segment.meta.owner_id.load(std::memory_order_acquire) !=
                    msg.worker_id ||
                object_index >=
                    segment.meta.tail_idx.load(std::memory_order_acquire)) {
                continue;
            }
            std::atomic_thread_fence(std::memory_order_acquire);
            const ObjectEntry& object = segment.entries[object_index];
            if (object.term_id == 0 || object.seq_num == 0 ||
                object.key[0] == '\0' ||
                std::tie(object.term_id, object.seq_num) >
                    std::tie(msg.incarnation, msg.object_index)) {
                continue;
            }
            ReplicationEntry entry;
            entry.sequence = object.seq_num;
            entry.incarnation = object.incarnation;
            entry.segment_index = segment_index;
            entry.object_index = object_index;
            entry.key = std::string(object.key);
            entry.value = object.value;
            entry.term = object.term_id;
            entry.worker_id = msg.worker_id;
            const size_t entry_bytes = entry.key.size() + entry.value.size() +
                                       8U * sizeof(uint64_t);
            if (!reply.entries.empty() &&
                reply_bytes + entry_bytes > kMaxWireRecoveryBatchBytes) {
                cursor = flat_index;
                break;
            }
            reply_bytes += entry_bytes;
            reply.entries.push_back(std::move(entry));
        }
        reply.seq = cursor;
        reply.object_index = cursor == total ? 1 : 0;
        reply.status = OperationStatus::OK;
        send_message(sock, reply);
    }

    void handle_recovery_install(int sock, const NetMessage& msg) {
        std::unique_lock<std::shared_mutex> epoch_lock(
            replication_epoch_mutex
        );
        NetMessage reply;
        reply.type = MsgType::ACK;
        reply.term = rs.current_term.load(std::memory_order_acquire);
        const bool authorized = rs.role.load(std::memory_order_acquire) == Role::FOLLOWER &&
            msg.term == reply.term &&
            msg.segment_index == recovery_leader_id && !msg.entries.empty();
        if (!authorized) {
            DISKEYV_WARN("RECOVERY",
                         "replica=" << rs.replica_id
                                    << " rejected=install-batch term="
                                    << msg.term << " candidate="
                                    << msg.segment_index << " entries="
                                    << msg.entries.size());
            reply.status = OperationStatus::INVALID_REQUEST;
            send_message(sock, reply);
            return;
        }

        reply.status = OperationStatus::OK;
        for (const ReplicationEntry& entry : msg.entries) {
            if (entry.worker_id >= kMaxReplicationWorkers ||
                entry.term == 0 || entry.sequence == 0 ||
                entry.key.empty() ||
                entry.key.size() > ObjectEntry::kMaxKeySize) {
                reply.status = OperationStatus::INVALID_REQUEST;
                break;
            }
            if (!install_recovery_record(entry)) {
                reply.status = OperationStatus::STORAGE_ERROR;
                break;
            }
        }
        if (reply.status == OperationStatus::OK) {
            reconstruct_worker_progress_locked();
        } else {
            DISKEYV_WARN("RECOVERY",
                         "replica=" << rs.replica_id
                                    << " install-batch status="
                                    << status_name(reply.status)
                                    << " entries=" << msg.entries.size());
        }
        send_message(sock, reply);
    }

    // Fence live replication into a new term and remember which recovery
    // candidate is authorized to fetch/install data in that term.
    void handle_term_advance(int sock, const NetMessage& msg) {
        std::unique_lock<std::shared_mutex> epoch_lock(
            replication_epoch_mutex
        );
        NetMessage reply;
        reply.type = MsgType::TERM_ADVANCE_REPLY;
        const uint64_t current = rs.current_term.load(std::memory_order_acquire);
        const bool valid_role =
            rs.role.load(std::memory_order_acquire) == Role::FOLLOWER;
        if (!valid_role || msg.term < current || msg.incarnation == 0 ||
            (msg.term == current && recovery_leader_id != 0 &&
             recovery_leader_id != msg.incarnation)) {
            DISKEYV_WARN("RECOVERY",
                         "replica=" << rs.replica_id
                                    << " rejected=term-advance current="
                                    << current << " requested=" << msg.term
                                    << " candidate=" << msg.incarnation);
            reply.status = OperationStatus::INVALID_REQUEST;
            reply.term = current;
            send_message(sock, reply);
            return;
        }

        recovery_leader_id = msg.incarnation;
        if (msg.term > current) {
            for (Segment* segment : store.segments) {
                if (segment->meta.status.load(std::memory_order_acquire) == static_cast<uint8_t>(SegmentStatus::ACTIVE) &&
                    segment->meta.term_id.load(std::memory_order_acquire) < msg.term) {
                    segment->seal();
                }
            }
            rs.current_term.store(msg.term, std::memory_order_release);
        }
        DISKEYV_INFO("RECOVERY",
                     "replica=" << rs.replica_id << " phase=term-advanced from="
                                << current << " to=" << msg.term
                                << " candidate=" << msg.incarnation);
        reply.term = msg.term;
        reply.status = OperationStatus::OK;
        send_message(sock, reply);
    }

    // Rebuild follower ACK state by grouping stored entries by worker and term,
    // then retaining the newest term's longest gap-free sequence prefix.
    std::vector<WorkerProgressWire> reconstruct_worker_progress_locked() {
        std::map<uint64_t, std::map<uint64_t, std::set<uint64_t>>> sequences;
        for (const Segment* segment : store.segments) {
            if (segment->meta.status.load(std::memory_order_acquire) == static_cast<uint8_t>(SegmentStatus::FREE)) {
                continue;
            }
            const uint64_t worker_id = segment->meta.owner_id.load(std::memory_order_acquire);
            if (worker_id >= kMaxReplicationWorkers) continue;
            const uint64_t tail = std::min(
                segment->capacity,
                segment->meta.tail_idx.load(std::memory_order_acquire)
            );
            std::atomic_thread_fence(std::memory_order_acquire);
            for (uint64_t index = 0; index < tail; ++index) {
                const ObjectEntry& object = segment->entries[index];
                if (object.term_id > 0 && object.seq_num > 0 &&
                    object.key[0] != '\0') {
                    sequences[worker_id][object.term_id].insert(object.seq_num);
                }
            }
        }

        std::vector<WorkerProgressWire> result;
        for (const auto& worker : sequences) {
            WorkerProgressWire latest;
            latest.worker_id = worker.first;
            for (const auto& term : worker.second) {
                uint64_t contiguous = 0;
                for (uint64_t sequence : term.second) {
                    if (sequence != contiguous + 1) break;
                    contiguous = sequence;
                }
                if (contiguous > 0 &&
                    (term.first > latest.term ||
                     (term.first == latest.term &&
                      contiguous > latest.sequence))) {
                    latest.term = term.first;
                    latest.sequence = contiguous;
                }
            }
            if (latest.sequence > 0) result.push_back(latest);
        }

        std::lock_guard<std::mutex> map_lock(follower_progress_mutex);
        follower_progress.clear();
        for (const WorkerProgressWire& progress : result) {
            auto value = std::make_shared<FollowerProgress>();
            value->term = progress.term;
            value->highest_contiguous_sequence = progress.sequence;
            follower_progress.emplace(progress.worker_id, std::move(value));
        }
        return result;
    }

    // Recovery is idempotent: an occupied offset must contain exactly the same
    // record; an empty offset is reproduced through the replicated put path.
    bool install_recovery_record(const ReplicationEntry& entry) {
        if (entry.segment_index >= store.segments.size()) return false;
        Segment* segment = store.segments[entry.segment_index];
        const uint64_t tail = segment->meta.tail_idx.load(std::memory_order_acquire);
        if (entry.object_index < tail) {
            NetMessage existing;
            existing.term = entry.term;
            existing.seq = entry.sequence;
            existing.incarnation = entry.incarnation;
            existing.worker_id = entry.worker_id;
            existing.segment_index = entry.segment_index;
            existing.object_index = entry.object_index;
            existing.key = entry.key;
            existing.value = entry.value;
            return replicated_entry_matches(existing);
        }

        ApplyRecord applied{};
        if (!PutPath::put_replicated_at(
                store, entry.worker_id,
                static_cast<size_t>(entry.segment_index), entry.object_index,
                entry.key, entry.value, entry.term, entry.sequence,
                entry.incarnation, applied, true)) {
            return false;
        }
        return index_apply_succeeded(
            ht.apply(store, applied.seg_idx, applied.obj_idx));
    }

    // Advance one worker's follower prefix by exactly one. Duplicate entries
    // are accepted only when every immutable field matches the stored object.
    OperationStatus apply_replication_record(const NetMessage& msg,
                                             FollowerProgress& progress) {
        if (progress.term > msg.term) {
            return OperationStatus::INVALID_REQUEST;
        }
        // A new term starts a fresh sequence namespace for this worker.
        const uint64_t prefix =
            progress.term == msg.term
                ? progress.highest_contiguous_sequence
                : 0;
        if (msg.seq <= prefix) {
            return replicated_entry_matches(msg) ? OperationStatus::OK
                                                 : OperationStatus::INVALID_REQUEST;
        }
        if (msg.seq != prefix + 1) {
            return OperationStatus::OUT_OF_ORDER;
        }

        ApplyRecord record{};
        const bool stored = PutPath::put_replicated_at(
            store, msg.worker_id, static_cast<size_t>(msg.segment_index),
            msg.object_index, msg.key, msg.value, msg.term, msg.seq,
            msg.incarnation, record);
        if (!stored) return OperationStatus::STORAGE_ERROR;

        ht.apply(store, record.seg_idx, record.obj_idx);
        progress.term = msg.term;
        progress.highest_contiguous_sequence = msg.seq;
        return OperationStatus::OK;
    }

    bool replicated_entry_matches(const NetMessage& msg) const {
        if (msg.segment_index >= store.segments.size()) return false;
        const Segment* segment = store.segments[msg.segment_index];
        if (msg.object_index >= segment->capacity ||
            msg.object_index >= segment->meta.tail_idx.load(std::memory_order_acquire)) {
            return false;
        }
        std::atomic_thread_fence(std::memory_order_acquire);
        const ObjectEntry& entry = segment->entries[msg.object_index];
        return entry.term_id == msg.term && entry.seq_num == msg.seq &&
               entry.incarnation == msg.incarnation &&
               std::string(entry.key) == msg.key && entry.value == msg.value;
    }

    std::shared_ptr<FollowerProgress> progress_for(uint64_t worker_id) {
        std::lock_guard<std::mutex> lock(follower_progress_mutex);
        auto& progress = follower_progress[worker_id];
        if (!progress) progress = std::make_shared<FollowerProgress>();
        return progress;
    }

    static void* worker_entry(void* arg) {
        auto* context = static_cast<std::pair<Replica*, uint64_t>*>(arg);
        Replica* replica = context->first;
        const uint64_t worker_id = context->second;
        delete context;
        Worker worker(worker_id);
        replica->worker_loop(worker);
        return nullptr;
    }

    void start_workers(int count) {
        for (int i = 0; i < count; ++i) {
            pthread_t thread{};
            auto* context = new std::pair<Replica*, uint64_t>(
                this, static_cast<uint64_t>(i));
            if (pthread_create(&thread, nullptr, worker_entry, context) != 0) {
                DISKEYV_ERROR("REPLICA",
                              "replica=" << rs.replica_id
                                         << " failed-to-start=worker id=" << i);
                delete context;
                stop.store(true, std::memory_order_release);
                pthread_cond_broadcast(&req_cv);
                for (pthread_t started : workers) pthread_join(started, nullptr);
                const int listening_socket =
                    server_sock.exchange(-1, std::memory_order_acq_rel);
                if (listening_socket >= 0) close(listening_socket);
                throw std::runtime_error("failed to start worker thread");
            }
            workers.push_back(thread);
        }
    }

    // Consume bounded client batches, append immutable records locally,
    // replicate them in parallel, and publish only the quorum-covered prefix.
    void worker_loop(Worker& worker) {
        DISKEYV_DEBUG("WORKER",
                      "replica=" << rs.replica_id << " worker="
                                 << worker.worker_id << " state=started");
        ParallelReplicationGroup replication(
            rs.current_term.load(std::memory_order_acquire), 
            worker.worker_id,
            peer_endpoints
        );
        std::deque<ApplyRecord> pending;
        uint64_t produced_sequence = 0;

        while (true) {
            pthread_mutex_lock(&req_mtx);
            while (!stop.load(std::memory_order_acquire) && req_q.empty()) {
                pthread_cond_wait(&req_cv, &req_mtx);
            }
            if (stop.load(std::memory_order_acquire) && req_q.empty()) {
                pthread_mutex_unlock(&req_mtx);
                DISKEYV_DEBUG("WORKER",
                              "replica=" << rs.replica_id << " worker="
                                         << worker.worker_id
                                         << " state=stopped");
                return;
            }
            std::vector<Request> requests;
            size_t request_bytes = 0;
            while (!req_q.empty() &&
                   requests.size() < kReplicationBatchEntryLimit) {
                const Request& candidate = req_q.front();
                const size_t candidate_bytes = candidate.key.size() +
                                               candidate.value.size() +
                                               6U * sizeof(uint64_t);
                if (!requests.empty() &&
                    request_bytes + candidate_bytes >
                        kReplicationBatchByteLimit) {
                    break;
                }
                request_bytes += candidate_bytes;
                requests.push_back(std::move(req_q.front()));
                req_q.pop();
            }
            pthread_mutex_unlock(&req_mtx);
            pthread_cond_broadcast(&req_space_cv);

            struct PreparedPut {
                Request request;
                ApplyRecord record;
                uint64_t sequence;
            };
            std::vector<PreparedPut> prepared;
            prepared.reserve(requests.size());
            for (Request& request : requests) {
                ApplyRecord record{};
                if (!PutPath::put(worker, rs, store, inc, request.key,
                                  request.value, record)) {
                    DISKEYV_WARN("STORAGE",
                                 "replica=" << rs.replica_id << " worker="
                                            << worker.worker_id
                                            << " put=out-of-space key-bytes="
                                            << request.key.size());
                    request.completion->set_value(OperationStatus::OUT_OF_SPACE);
                    continue;
                }
                const uint64_t sequence =
                    store.segments[record.seg_idx]->entries[record.obj_idx]
                        .seq_num;
                if (sequence != produced_sequence + 1) {
                    DISKEYV_ERROR("STORAGE",
                                  "replica=" << rs.replica_id << " worker="
                                             << worker.worker_id
                                             << " noncontiguous-sequence expected="
                                             << produced_sequence + 1
                                             << " actual=" << sequence);
                    request.completion->set_value(OperationStatus::STORAGE_ERROR);
                    continue;
                }
                produced_sequence = sequence;
                pending.push_back(record);
                prepared.push_back(
                    PreparedPut{std::move(request), record, sequence});
            }
            if (prepared.empty()) continue;

            std::vector<ApplyRecord> replication_records;
            replication_records.reserve(prepared.size());
            for (const PreparedPut& put : prepared) {
                replication_records.push_back(put.record);
            }
            const ParallelReplicationGroup::Ticket ticket =
                replication.publish(replication_records, store);
            const uint64_t commit_watermark =
                replication.wait_for_commit(ticket, quorum);
            std::unordered_map<uint64_t, IndexApplyResult> publications;

            // pending can contain earlier locally appended records that did
            // not reach quorum. A later cumulative watermark may publish them.
            while (!pending.empty()) {
                const ApplyRecord& candidate = pending.front();
                const ObjectEntry& entry =
                    store.segments[candidate.seg_idx]->entries[candidate.obj_idx];
                if (entry.seq_num > commit_watermark) break;
                const IndexApplyResult publication =
                    ht.apply(store, candidate.seg_idx, candidate.obj_idx);
                publications.emplace(entry.seq_num, publication);
                pending.pop_front();
            }

            for (PreparedPut& put : prepared) {
                OperationStatus result = OperationStatus::NO_QUORUM;
                if (put.sequence <= commit_watermark) {
                    const auto publication = publications.find(put.sequence);
                    if (publication == publications.end() ||
                        publication->second == IndexApplyResult::INVALID) {
                        result = OperationStatus::STORAGE_ERROR;
                    } else if (publication->second == IndexApplyResult::FULL) {
                        result = OperationStatus::INDEX_FULL;
                    } else {
                        result = OperationStatus::OK;
                    }
                }
                put.request.completion->set_value(result);
                if (result != OperationStatus::OK &&
                    result != OperationStatus::NO_QUORUM) {
                    DISKEYV_WARN("WORKER",
                                 "replica=" << rs.replica_id << " worker="
                                            << worker.worker_id
                                            << " sequence=" << put.sequence
                                            << " status=" << status_name(result));
                }
            }
        }
    }

    // Apply backpressure at the network boundary and synchronously return the
    // result produced by whichever worker dequeues this request.
    OperationStatus submit_put(const std::string& key,
                               const std::vector<uint8_t>& value) {
        if (recovering.load(std::memory_order_acquire)) {
            return OperationStatus::RECOVERING;
        }
        if (rs.role.load(std::memory_order_acquire) != Role::LEADER) {
            return OperationStatus::NOT_LEADER;
        }
        if (key.empty() || key.size() > ObjectEntry::kMaxKeySize ||
            value.size() > kMaxWireValueSize) {
            return OperationStatus::INVALID_REQUEST;
        }
        if (stop.load(std::memory_order_acquire)) {
            return OperationStatus::SHUTTING_DOWN;
        }

        auto completion = std::make_shared<std::promise<OperationStatus>>();
        std::future<OperationStatus> result = completion->get_future();
        pthread_mutex_lock(&req_mtx);
        while (!stop.load(std::memory_order_acquire) &&
               req_q.size() >= kRequestQueueLimit) {
            pthread_cond_wait(&req_space_cv, &req_mtx);
        }
        if (stop.load(std::memory_order_acquire)) {
            pthread_mutex_unlock(&req_mtx);
            return OperationStatus::SHUTTING_DOWN;
        }
        req_q.push(Request{key, value, completion});
        pthread_mutex_unlock(&req_mtx);
        pthread_cond_signal(&req_cv);
        return result.get();
    }

    ObjectEntry* get(const std::string& key) { return ht.get(store, key); }

    int listening_port() const {
        sockaddr_in address{};
        socklen_t length = sizeof(address);
        const int listening_socket = server_sock.load(std::memory_order_acquire);
        if (listening_socket < 0 ||
            getsockname(listening_socket, reinterpret_cast<sockaddr*>(&address),
                        &length) < 0) {
            return -1;
        }
        return static_cast<int>(ntohs(address.sin_port));
    }

    uint64_t accepted_connection_count() const {
        return accepted_connections.load(std::memory_order_acquire);
    }

    uint64_t received_batch_count() const {
        return received_batches.load(std::memory_order_acquire);
    }

    uint64_t largest_batch_count() const {
        return largest_received_batch.load(std::memory_order_acquire);
    }

    void shutdown() {
        if (shutdown_called.exchange(true, std::memory_order_acq_rel)) return;
        DISKEYV_INFO("REPLICA",
                     "replica=" << rs.replica_id << " phase=shutdown-start");
        stop.store(true, std::memory_order_release);

        const int listening_socket =
            server_sock.exchange(-1, std::memory_order_acq_rel);
        if (listening_socket >= 0) {
            ::shutdown(listening_socket, SHUT_RDWR);
            close(listening_socket);
        }
        pthread_cond_broadcast(&req_cv);
        pthread_cond_broadcast(&req_space_cv);
        pthread_join(net_thread, nullptr);
        for (pthread_t thread : workers) pthread_join(thread, nullptr);

        {
            std::lock_guard<std::mutex> lock(connections_mutex);
            for (const auto& connection : connections) {
                const int socket =
                    connection->socket.load(std::memory_order_acquire);
                if (socket >= 0) {
                    ::shutdown(socket, SHUT_RDWR);
                }
            }
        }
        for (const auto& connection : connections) {
            pthread_join(connection->thread, nullptr);
        }
        connections.clear();
        DISKEYV_INFO("REPLICA",
                     "replica=" << rs.replica_id << " phase=shutdown-complete"
                                << " connections="
                                << accepted_connections.load(
                                       std::memory_order_relaxed)
                                << " batches="
                                << received_batches.load(
                                       std::memory_order_relaxed));
    }
};
