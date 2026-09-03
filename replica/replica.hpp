#pragma once

#include <atomic>
#include <future>
#include <iostream>
#include <memory>
#include <pthread.h>
#include <queue>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include "replica_state.hpp"
#include "../concurrency/incarnation.hpp"
#include "../engine/put_path.hpp"
#include "../engine/worker.hpp"
#include "../index/hash_table.hpp"
#include "../network/message.hpp"
#include "../network/socket_utils.hpp"
#include "../storage/segment_store.hpp"

struct Request {
    std::string key;
    std::vector<uint8_t> value;
    std::shared_ptr<std::promise<OperationStatus>> completion;
};

struct Replica {
    ReplicaState rs;
    SegmentStore store;
    HashTable ht;
    IncarnationTable inc;

    std::vector<pthread_t> workers;
    std::queue<Request> req_q;
    pthread_mutex_t req_mtx = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t req_cv = PTHREAD_COND_INITIALIZER;

    std::atomic<bool> stop{false};
    std::atomic<bool> shutdown_called{false};
    int server_sock{-1};
    std::vector<int> peer_ports;
    size_t quorum{1};
    pthread_t net_thread{};

    Replica(uint64_t rid, int nworkers, Role role, int port,
            const std::vector<int>& peer_ports_list,
            size_t segment_count = 32, uint64_t segment_capacity = 1024)
        : rs(rid, role),
          store(segment_count, segment_capacity),
          ht(4096),
          inc(2048),
          peer_ports(peer_ports_list) {
        if (nworkers <= 0) {
            throw std::invalid_argument("a replica needs at least one worker");
        }
        server_sock = create_server(port);
        if (server_sock < 0) {
            throw std::runtime_error("failed to create replica server socket");
        }
        if (role == Role::LEADER) {
            quorum = (peer_ports.size() + 1) / 2 + 1;
        }

        start_workers(nworkers);
        start_network_thread();
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

    void start_network_thread() {
        if (pthread_create(&net_thread, nullptr, net_entry, this) != 0) {
            stop.store(true, std::memory_order_release);
            pthread_cond_broadcast(&req_cv);
            for (pthread_t thread : workers) pthread_join(thread, nullptr);
            close(server_sock);
            server_sock = -1;
            throw std::runtime_error("failed to start network thread");
        }
    }

    void network_loop() {
        while (!stop.load(std::memory_order_acquire)) {
            sockaddr_in client_addr{};
            socklen_t len = sizeof(client_addr);
            const int client = accept(
                server_sock, reinterpret_cast<sockaddr*>(&client_addr), &len);
            if (client < 0) {
                if (stop.load(std::memory_order_acquire)) return;
                continue;
            }
            handle_connection(client);
        }
    }

    void handle_connection(int sock) {
        NetMessage msg;
        if (!recv_message(sock, msg)) {
            close(sock);
            return;
        }

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
            default: {
                NetMessage reply;
                reply.type = MsgType::ACK;
                reply.status = OperationStatus::INVALID_REQUEST;
                send_message(sock, reply);
                break;
            }
        }
        close(sock);
    }

    void handle_client_put(int sock, const NetMessage& msg) {
        NetMessage reply;
        reply.type = MsgType::CLIENT_PUT_REPLY;
        if (rs.role.load(std::memory_order_acquire) != Role::LEADER) {
            reply.status = OperationStatus::NOT_LEADER;
        } else {
            reply.status = submit_put(msg.key, msg.value);
        }
        send_message(sock, reply);
    }

    void handle_client_get(int sock, const NetMessage& msg) {
        NetMessage reply;
        reply.type = MsgType::CLIENT_GET_REPLY;
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
        NetMessage reply;
        reply.type = MsgType::ACK;
        reply.term = msg.term;
        reply.seq = msg.seq;
        reply.worker_id = msg.worker_id;

        ApplyRecord record{};
        const bool valid_request =
            rs.role.load(std::memory_order_acquire) == Role::FOLLOWER &&
            !msg.key.empty() && msg.key.size() <= ObjectEntry::kMaxKeySize;
        const bool stored = valid_request && PutPath::put_replicated_at(
            store, msg.worker_id, static_cast<size_t>(msg.segment_index),
            msg.object_index, msg.key, msg.value, msg.term, msg.seq,
            msg.incarnation, record);

        if (stored) {
            ht.apply(store, record.seg_idx, record.obj_idx);
            reply.status = OperationStatus::OK;
        } else {
            reply.status = valid_request ? OperationStatus::STORAGE_ERROR
                                         : OperationStatus::INVALID_REQUEST;
        }
        send_message(sock, reply);
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
                delete context;
                stop.store(true, std::memory_order_release);
                pthread_cond_broadcast(&req_cv);
                for (pthread_t started : workers) pthread_join(started, nullptr);
                close(server_sock);
                server_sock = -1;
                throw std::runtime_error("failed to start worker thread");
            }
            workers.push_back(thread);
        }
    }

    void worker_loop(Worker& worker) {
        while (true) {
            pthread_mutex_lock(&req_mtx);
            while (!stop.load(std::memory_order_acquire) && req_q.empty()) {
                pthread_cond_wait(&req_cv, &req_mtx);
            }
            if (stop.load(std::memory_order_acquire) && req_q.empty()) {
                pthread_mutex_unlock(&req_mtx);
                return;
            }
            Request request = std::move(req_q.front());
            req_q.pop();
            pthread_mutex_unlock(&req_mtx);

            ApplyRecord record{};
            if (!PutPath::put(worker, rs, store, inc, request.key,
                              request.value, record)) {
                request.completion->set_value(OperationStatus::OUT_OF_SPACE);
                continue;
            }
            if (!replicate_to_followers(record)) {
                request.completion->set_value(OperationStatus::NO_QUORUM);
                continue;
            }

            ht.apply(store, record.seg_idx, record.obj_idx);
            request.completion->set_value(OperationStatus::OK);
        }
    }

    OperationStatus submit_put(const std::string& key,
                               const std::vector<uint8_t>& value) {
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
        if (getsockname(server_sock, reinterpret_cast<sockaddr*>(&address),
                        &length) < 0) {
            return -1;
        }
        return static_cast<int>(ntohs(address.sin_port));
    }

    bool replicate_to_followers(const ApplyRecord& record) {
        Segment* segment = store.segments[record.seg_idx];
        ObjectEntry& object = segment->entries[record.obj_idx];

        NetMessage message;
        message.type = MsgType::PUT_REPL;
        message.term = object.term_id;
        message.seq = object.seq_num;
        message.incarnation = object.incarnation;
        message.worker_id = record.worker_id;
        message.segment_index = record.seg_idx;
        message.object_index = record.obj_idx;
        message.key = std::string(object.key);
        message.value = object.value;

        size_t acknowledgements = 1;
        for (int port : peer_ports) {
            const int sock = connect_to("127.0.0.1", port);
            if (sock < 0) continue;

            NetMessage reply;
            if (send_message(sock, message) && recv_message(sock, reply) &&
                reply.type == MsgType::ACK &&
                reply.status == OperationStatus::OK &&
                reply.term == message.term && reply.seq == message.seq &&
                reply.worker_id == message.worker_id) {
                ++acknowledgements;
            }
            close(sock);
        }
        return acknowledgements >= quorum;
    }

    void shutdown() {
        if (shutdown_called.exchange(true, std::memory_order_acq_rel)) return;
        stop.store(true, std::memory_order_release);

        if (server_sock >= 0) {
            ::shutdown(server_sock, SHUT_RDWR);
            close(server_sock);
            server_sock = -1;
        }
        pthread_cond_broadcast(&req_cv);
        for (pthread_t thread : workers) pthread_join(thread, nullptr);
        pthread_join(net_thread, nullptr);
    }
};
