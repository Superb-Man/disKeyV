#pragma once
#include <pthread.h>
#include <queue>
#include <vector>
#include <string>
#include <iostream>
#include <atomic>
#include <unordered_map>
#include <unistd.h>

#include "replica_state.hpp"
#include "../engine/worker.hpp"
#include "../engine/put_path.hpp"
#include "../storage/segment_store.hpp"
#include "../index/hash_table.hpp"
#include "../concurrency/incarnation.hpp"
#include "../network/socket_utils.hpp"
#include "../network/message.hpp"

enum class OpType { PUT, TX_PUT };

struct Request {
    OpType type;
    std::string key;
    std::vector<uint8_t> value;
    uint64_t tx_id = 0;
    std::vector<TxKeyValue> kv_pairs;
};

enum class Role { LEADER, FOLLOWER };

struct PendingTx {
    std::vector<TxKeyValue> staged_kv;
    uint64_t term;
    uint64_t seq;
    uint64_t incarnation;
    std::atomic<bool> prepared{false};
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

    pthread_t apply_thread;
    std::queue<ApplyRecord> apply_q;
    pthread_mutex_t apply_mtx = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t apply_cv = PTHREAD_COND_INITIALIZER;

    std::unordered_map<uint64_t, PendingTx> pending_txs;
    pthread_mutex_t tx_mtx = PTHREAD_MUTEX_INITIALIZER;

    bool stop = false;
    bool shutdown_called = false;
    Role role;
    int server_sock;
    std::vector<int> peer_ports;
    size_t quorum;
    pthread_t net_thread;
    static std::atomic<uint64_t> global_tx_id;

    Replica(uint64_t rid, int nworkers, Role r, int port, const std::vector<int>& peer_ports_list)
        : rs(rid), store(32, 1024), ht(4096), inc(2048), role(r), peer_ports(peer_ports_list) {

        server_sock = create_server(port);
        std::cout << "[Replica " << rid << "] Listening on port " << port 
                  << " (role: " << (r == Role::LEADER ? "LEADER" : "FOLLOWER") << ")\n";

        if (role == Role::LEADER) {
            quorum = (peer_ports.size() + 1) / 2 + 1;
            std::cout << "[Leader] Quorum set to " << quorum 
                      << " (with " << peer_ports.size() << " followers)\n";
        }

        start_workers(nworkers);
        start_apply_thread();
        start_network_thread();
    }

    ~Replica() {
        shutdown();
    }

    // Generate proper metadata and write locally
    ApplyRecord write_local(const std::string& key, const std::vector<uint8_t>& value) {
        Worker dummy(0);
        ApplyRecord ar;
        PutPath::put(dummy, rs, store, inc, key, value, ar);
        return ar;
    }

    // Network thread entry point
    static void* net_entry(void* arg) {
        reinterpret_cast<Replica*>(arg)->network_loop();
        return nullptr;
    }

    void start_network_thread() {
        pthread_create(&net_thread, nullptr, net_entry, this);
        std::cout << "[Replica] Network thread started\n";
    }

    void network_loop() {
        std::cout << "[Network] Entering accept loop\n";
        while (!stop) {
            sockaddr_in client_addr{};
            socklen_t len = sizeof(client_addr);
            int client = accept(server_sock, (sockaddr*)&client_addr, &len);
            if (client < 0) continue;

            std::cout << "[Network] New client connection accepted\n";
            handle_connection(client);
        }
    }

    // Handle a single incoming connection (one message per connection)
    void handle_connection(int sock) {
        NetMessage msg;

        if (!recv_message(sock, msg)) {
            std::cout << "[Network] Failed to receive message from client\n";
            close(sock);
            return;
        }

        std::cout << "[Network] Received message of type " << static_cast<int>(msg.type) << "\n";

        if (msg.type == MsgType::CLIENT_PUT) {
            if (role != Role::LEADER) {
                std::cout << "[Client] Rejected PUT request: this node is not the leader\n";
                close(sock);
                return;
            }

            std::cout << "[Client] Processing PUT for key: " << msg.key << "\n";
            submit_put(msg.key, msg.value);

            NetMessage reply;
            reply.type = MsgType::CLIENT_PUT_REPLY;
            send_message(sock, reply);
            std::cout << "[Client] Sent PUT reply\n";

        } else if (msg.type == MsgType::CLIENT_GET) {
            std::cout << "[Client] Processing GET for key: " << msg.key << "\n";
            ObjectEntry* e = get(msg.key);
            NetMessage reply;
            reply.type = MsgType::CLIENT_GET_REPLY;

            if (e) {
                reply.term = e->term_id;
                reply.seq = e->seq_num;
                reply.incarnation = e->incarnation;
                reply.value = e->value;
                std::cout << "[Client] Key found; sending reply\n";
            } else {
                std::cout << "[Client] Key not found\n";
            }

            send_message(sock, reply);

        } else if (msg.type == MsgType::PUT_REPL) {
            std::cout << "[Replication] Received replication message for key: " << msg.key
                      << " (term=" << msg.term << ", seq=" << msg.seq
                      << ", incarnation=" << msg.incarnation << ")\n";

            ApplyRecord ar;

            if (PutPath::put_replicated(
                    rs,
                    store,
                    msg.key,
                    msg.value,
                    msg.term,
                    msg.seq,
                    msg.incarnation,
                    ar)) {
                pthread_mutex_lock(&apply_mtx);
                apply_q.push(ar);
                pthread_mutex_unlock(&apply_mtx);
                pthread_cond_signal(&apply_cv);
                std::cout << "[Replication] Apply record enqueued\n";
            }
            uint8_t ack = static_cast<uint8_t>(MsgType::ACK);
            send_all(sock, &ack, 1);
            std::cout << "[Replication] ACK sent\n";

        } else if (msg.type == MsgType::CLIENT_TX_PUT) {
            if (role != Role::LEADER) {
                std::cout << "[Client] Rejected TX_PUT request: this node is not the leader\n";
                close(sock);
                return;
            }

            std::cout << "[Client] Processing TX_PUT for " << msg.kv_pairs.size() << " keys\n";
            bool success = submit_tx_put(msg.kv_pairs);

            NetMessage reply;
            reply.type = MsgType::CLIENT_TX_PUT_REPLY;
            reply.term = success ? 1 : 0;
            send_message(sock, reply);
            std::cout << "[Client] Sent TX_PUT reply (" 
                      << (success ? "COMMITTED" : "ABORTED") << ")\n";

        } else if (msg.type == MsgType::TX_PREPARE) {
            std::cout << "[2PL] Received TX_PREPARE for tx_id=" << msg.tx_id 
                      << " with " << msg.kv_pairs.size() << " keys\n";

            pthread_mutex_lock(&tx_mtx);
            PendingTx& pt = pending_txs[msg.tx_id];
            pt.staged_kv = msg.kv_pairs;
            pt.term = msg.term;
            pt.seq = msg.seq;
            pt.incarnation = msg.incarnation;
            pt.prepared.store(true);
            pthread_mutex_unlock(&tx_mtx);

            NetMessage reply;
            reply.type = MsgType::TX_PREPARE_OK;
            reply.tx_id = msg.tx_id;
            send_message(sock, reply);
            std::cout << "[2PL] Sent TX_PREPARE_OK\n";

        } else if (msg.type == MsgType::TX_COMMIT) {
            std::cout << "[2PL] Received TX_COMMIT for tx_id=" << msg.tx_id << "\n";

            pthread_mutex_lock(&tx_mtx);
            auto it = pending_txs.find(msg.tx_id);
            if (it != pending_txs.end()) {
                const PendingTx& pt = it->second;
                for (const auto& kv : pt.staged_kv) {
                    ApplyRecord ar;
                    if (PutPath::put_replicated(rs, store, kv.key, kv.value,
                                               pt.term, pt.seq, pt.incarnation, ar)) {
                        pthread_mutex_lock(&apply_mtx);
                        apply_q.push(ar);
                        pthread_mutex_unlock(&apply_mtx);
                        pthread_cond_signal(&apply_cv);
                    }
                }
                pending_txs.erase(it);
                std::cout << "[2PL] Committed transaction\n";
            }
            pthread_mutex_unlock(&tx_mtx);

            uint8_t ack = static_cast<uint8_t>(MsgType::ACK);
            send_all(sock, &ack, 1);
            std::cout << "[2PL] Sent COMMIT ACK\n";

        } else if (msg.type == MsgType::TX_ABORT) {
            std::cout << "[2PL] Received TX_ABORT for tx_id=" << msg.tx_id << "\n";

            pthread_mutex_lock(&tx_mtx);
            pending_txs.erase(msg.tx_id);
            pthread_mutex_unlock(&tx_mtx);

            uint8_t ack = static_cast<uint8_t>(MsgType::ACK);
            send_all(sock, &ack, 1);
            std::cout << "[2PL] Sent ABORT ACK\n";
        }

        close(sock);
        std::cout << "[Network] Connection closed\n";
    }

    // Worker thread 
    static void* worker_entry(void* arg) {
        auto* ctx = static_cast<std::pair<Replica*, uint64_t>*>(arg);
        Replica* self = ctx->first;
        uint64_t wid = ctx->second;
        delete ctx;

        std::cout << "[Worker " << wid << "] Worker thread started\n";
        Worker w(wid);
        self->worker_loop(w);
        return nullptr;
    }

    void start_workers(int n) {
        for (int i = 0; i < n; i++) {
            pthread_t t;
            auto* ctx = new std::pair<Replica*, uint64_t>(this, i);
            pthread_create(&t, nullptr, worker_entry, ctx);
            workers.push_back(t);
        }
        std::cout << "[Replica] Started " << n << " worker threads\n";
    }

    void worker_loop(Worker& w) {
        while (true) {
            Request req;
            pthread_mutex_lock(&req_mtx);

            while (!stop && req_q.empty()) {
                pthread_cond_wait(&req_cv, &req_mtx);
            }

            if (stop && req_q.empty()) {
                pthread_mutex_unlock(&req_mtx);
                std::cout << "[Worker " << w.worker_id << "] Worker thread exiting\n";
                return;
            }

            req = req_q.front();
            req_q.pop();
            pthread_mutex_unlock(&req_mtx);

            if (req.type == OpType::PUT) {
                std::cout << "[Worker " << w.worker_id << "] Processing PUT for key: " << req.key << "\n";
                ApplyRecord ar;
                if (PutPath::put(w, rs, store, inc, req.key, req.value, ar)) {
                    if (role == Role::LEADER) {
                        std::cout << "[Leader] Initiating replication to followers\n";
                        if (!replicate_to_followers(ar)) {
                            std::cout << "[Leader] Replication failed; skipping apply\n";
                            continue;
                        }
                        std::cout << "[Leader] Replication succeeded\n";
                    }
                    pthread_mutex_lock(&apply_mtx);
                    apply_q.push(ar);
                    pthread_mutex_unlock(&apply_mtx);
                    pthread_cond_signal(&apply_cv);
                    std::cout << "[Worker " << w.worker_id << "] Apply record submitted\n";
                }
            }
        }
    }

    // Apply thread
    static void* apply_entry(void* arg) {
        reinterpret_cast<Replica*>(arg)->apply_loop();
        return nullptr;
    }

    void start_apply_thread() {
        pthread_create(&apply_thread, nullptr, apply_entry, this);
        std::cout << "[Replica] Apply thread started\n";
    }

    void apply_loop() {
        std::cout << "[Apply] Apply thread running\n";
        while (true) {
            ApplyRecord ar;
            pthread_mutex_lock(&apply_mtx);

            while (!stop && apply_q.empty())
                pthread_cond_wait(&apply_cv, &apply_mtx);

            if (stop && apply_q.empty()) {
                pthread_mutex_unlock(&apply_mtx);
                std::cout << "[Apply] Apply thread exiting\n";
                return;
            }

            ar = apply_q.front();
            apply_q.pop();
            pthread_mutex_unlock(&apply_mtx);

            std::cout << "[Apply] Applying object at segment " << ar.seg_idx 
                      << ", index " << ar.obj_idx << "\n";
            ht.apply(store, ar.seg_idx, ar.obj_idx);
            std::cout << "[Apply] Apply completed\n";
        }
    }

    
    void submit_put(const std::string& k, const std::vector<uint8_t>& v) {
        pthread_mutex_lock(&req_mtx);
        req_q.push({OpType::PUT, k, v});
        pthread_mutex_unlock(&req_mtx);
        pthread_cond_signal(&req_cv);
    }

    bool submit_tx_put(const std::vector<TxKeyValue>& kv_pairs) {
        if (role != Role::LEADER) return false;

        // Step 1: Generate all metadata locally (ensures consistent incarnations)
        std::vector<ApplyRecord> ars;
        std::vector<NetMessage> repl_msgs;
        
        Worker dummy_worker(0);
        for (const auto& kv : kv_pairs) {
            ApplyRecord ar;
            if (!PutPath::put(dummy_worker, rs, store, inc, kv.key, kv.value, ar)) {
                return false;
            }
            ars.push_back(ar);
            
            // Build replication message (same as single-key)
            Segment* seg = store.segments[ar.seg_idx];
            ObjectEntry& obj = seg->entries[ar.obj_idx];
            NetMessage msg;
            msg.type = MsgType::PUT_REPL;
            msg.term = obj.term_id;
            msg.seq = obj.seq_num;
            msg.incarnation = obj.incarnation;
            msg.key = kv.key;
            msg.value = kv.value;
            repl_msgs.push_back(msg);
        }

        // Step 2: Replicate all keys to followers (same path as single-key)
        size_t total_keys = kv_pairs.size();
        size_t total_acks = 0;
        
        for (size_t i = 0; i < total_keys; i++) {
            size_t ack_count = 1; // Leader counts itself
            
            for (int port : peer_ports) {
                int sock = connect_to("127.0.0.1", port);
                if (sock >= 0) {
                    if (send_message(sock, repl_msgs[i])) {
                        uint8_t ack;
                        if (recv_all(sock, &ack, 1) && ack == static_cast<uint8_t>(MsgType::ACK)) {
                            ack_count++;
                        }
                    }
                    close(sock);
                }
            }
            
            // Check quorum for this key
            if (ack_count < quorum) {
                return false; // Abort entire transaction
            }
            total_acks += ack_count;
        }

        // Step 3: Apply all keys locally (since all replicated successfully)
        for (const auto& ar : ars) {
            pthread_mutex_lock(&apply_mtx);
            apply_q.push(ar);
            pthread_mutex_unlock(&apply_mtx);
            pthread_cond_signal(&apply_cv);
        }
        
        return true;
    }

    ObjectEntry* get(const std::string& k) {
        return ht.get(store, k);
    }

    void shutdown() {
        pthread_mutex_lock(&req_mtx);

        if (shutdown_called) {
            pthread_mutex_unlock(&req_mtx);
            return;
        }

        shutdown_called = true;
        stop = true;
        pthread_mutex_unlock(&req_mtx);

        std::cout << "[Replica] Initiating shutdown\n";
        pthread_cond_broadcast(&req_cv);
        pthread_cond_broadcast(&apply_cv);

        for (auto& t : workers)
            pthread_join(t, nullptr);

        pthread_join(apply_thread, nullptr);
        pthread_join(net_thread, nullptr);
        close(server_sock);
        std::cout << "[Replica] Shutdown complete\n";
    }

    
    bool replicate_to_followers(const ApplyRecord& ar) {
        Segment* leader_seg = store.segments[ar.seg_idx];
        ObjectEntry& obj = leader_seg->entries[ar.obj_idx];

        NetMessage msg;
        msg.type = MsgType::PUT_REPL;
        msg.term = obj.term_id;
        msg.seq = obj.seq_num;
        msg.incarnation = obj.incarnation;
        msg.key = std::string(obj.key);
        msg.value = obj.value;

        size_t ack_count = 1;

        for (int port : peer_ports) {
            std::cout << "[Leader] Connecting to follower on port " << port << "\n";
            int sock = connect_to("127.0.0.1", port);

            if (sock < 0) {
                std::cout << "[Leader] Failed to connect to follower on port " << port << "\n";
                continue;
            }

            if (send_message(sock, msg)) {
                uint8_t ack;
                if (recv_all(sock, &ack, 1) && ack == static_cast<uint8_t>(MsgType::ACK)) {
                    ack_count++;
                    std::cout << "[Leader] Received ACK from follower on port " << port << "\n";
                } else {
                    std::cout << "[Leader] No ACK received from follower on port " << port << "\n";
                }
            } else {
                std::cout << "[Leader] Failed to send replication message to port " << port << "\n";
            }
            close(sock);
        }

        bool success = (ack_count >= quorum);
        std::cout << "[Leader] Replication result: received " << ack_count 
                  << " acknowledgments, quorum is " << quorum 
                  << " -> " << (success ? "committed" : "not committed") << "\n";
        return success;
    }
};

std::atomic<uint64_t> Replica::global_tx_id{0};