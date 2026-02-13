#pragma once
#include <pthread.h>
#include <queue>
#include <vector>
#include <string>
#include <iostream>
#include <atomic>

#include "replica_state.hpp"
#include "../engine/worker.hpp"
#include "../engine/put_path.hpp"
#include "../storage/segment_store.hpp"
#include "../index/hash_table.hpp"
#include "../concurrency/incarnation.hpp"
#include "../network/socket_utils.hpp"
#include "../network/message.hpp"

enum class OpType { PUT };

struct Request {
    OpType type;
    std::string key;
    std::vector<uint8_t> value;
};
struct Follower {

    uint64_t id;
    SegmentStore store;

    Follower(uint64_t rid,
             size_t nseg,
             uint64_t cap)
        : id(rid),
          store(nseg, cap) {}
};

enum class Role { LEADER, FOLLOWER };


struct Replica {
    ReplicaState rs;
    SegmentStore store;
    HashTable ht;
    IncarnationTable inc;

    //worker side
    std::vector<pthread_t> workers;
    std::queue<Request> req_q;
    pthread_mutex_t req_mtx;
    pthread_cond_t req_cv;

    //apply side
    pthread_t apply_thread;
    std::queue<ApplyRecord> apply_q;
    pthread_mutex_t apply_mtx;
    pthread_cond_t apply_cv;

    //control
    bool stop;
    bool shutdown_called;

    // Replication
    // size_t replica_count;
    // size_t quorum; // number of replicas required for majority
    // std::vector<Follower*> followers;
    Role role;
    int server_sock;
    std::vector<int> peer_socks;
    size_t quorum;

    pthread_t net_thread;

    Replica(uint64_t rid,
            int nworkers,
            Role r,
            int port,
            const std::vector<int>& peer_ports)
        : rs(rid),
        store(32,1024),
        ht(4096),
        inc(2048),
        role(r),
        stop(false),
        shutdown_called(false) {
        pthread_mutex_init(&req_mtx, nullptr);
    
        pthread_cond_init(&req_cv, nullptr);

        pthread_mutex_init(&apply_mtx, nullptr);
        pthread_cond_init(&apply_cv, nullptr);

        server_sock = create_server(port);

        if (role == Role::LEADER) {
            for (int p : peer_ports) {
                int sock = -1;
                while (sock < 0) {
                    sock = connect_to("127.0.0.1", p);

                    if (sock < 0) {
                        std::cout << "waiting for follower on port " << p << "...\n";
                        usleep(500000);
                    }
                }

                std::cout << "connected to follower on port " << p << "\n";
                peer_socks.push_back(sock);
            }

            quorum = (peer_socks.size() + 1) / 2 + 1;
        }

        start_workers(nworkers);
        start_apply_thread();

        start_network_thread();
    }

    static void* net_entry(void* arg) {
        reinterpret_cast<Replica*>(arg)->network_loop();
        return nullptr;
    }

    void start_network_thread() {
        pthread_create(&net_thread, nullptr, net_entry, this);
    }

    void network_loop() {
        while (!stop) {
            sockaddr_in client_addr{};
            socklen_t len = sizeof(client_addr);

            int client = accept(server_sock,
                                (sockaddr*)&client_addr,
                                &len);

            if (client < 0) continue;

            handle_connection(client);
            close(client);
        }
    }

    void handle_connection(int sock) {

        while (true) {

            NetMessage msg;

            if (!recv_message(sock, msg))
                break;  // client disconnected

            if (msg.type == MsgType::PUT_REPL) {

                ApplyRecord ar;

                if (!PutPath::put_replicated(
                        rs,
                        store,
                        msg.key,
                        msg.value,
                        msg.term,
                        msg.seq,
                        msg.incarnation,
                        ar))
                    continue;

                pthread_mutex_lock(&apply_mtx);
                apply_q.push(ar);
                pthread_mutex_unlock(&apply_mtx);
                pthread_cond_signal(&apply_cv);

                uint8_t ack = (uint8_t)MsgType::ACK;
                send_all(sock, &ack, 1);
            }
        }
    }



    ~Replica() {
        shutdown();

        pthread_mutex_destroy(&req_mtx);
        pthread_cond_destroy(&req_cv);

        pthread_mutex_destroy(&apply_mtx);
        pthread_cond_destroy(&apply_cv);

        // for (Follower* f : followers) {
        //     delete f;
        // }

        // followers.clear();
    }

    static void* worker_entry(void* arg) {
        struct std::pair<Replica *, uint64_t>* ctx = static_cast<std::pair<Replica*, uint64_t>*>(arg);
        Replica* self = ctx->first;
        uint64_t wid = ctx->second;
        delete ctx;

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
    }

    void worker_loop(Worker& w) {
        while (true) {
            Request req;

            pthread_mutex_lock(&req_mtx);
            while (!stop && req_q.empty())
                pthread_cond_wait(&req_cv, &req_mtx);

            if (stop && req_q.empty()) {
                pthread_mutex_unlock(&req_mtx);
                return;
            }

            req = req_q.front();
            req_q.pop();
            pthread_mutex_unlock(&req_mtx);

            if (req.type == OpType::PUT) {

                ApplyRecord ar;
                if (PutPath::put(w, rs, store, inc,
                                req.key, req.value, ar)) {

                    if (role == Role::LEADER) {

                        if (!replicate_to_followers(ar)) continue;
                    }

                    pthread_mutex_lock(&apply_mtx);
                    apply_q.push(ar);
                    pthread_mutex_unlock(&apply_mtx);
                    pthread_cond_signal(&apply_cv);
                }
            }

        }
    }

    /* ================= APPLY THREAD ================= */

    static void* apply_entry(void* arg) {
        reinterpret_cast<Replica*>(arg)->apply_loop();
        return nullptr;
    }

    void start_apply_thread() {
        pthread_create(&apply_thread, nullptr, apply_entry, this);
    }

    void apply_loop() {
        while (true) {
            ApplyRecord ar;

            pthread_mutex_lock(&apply_mtx);
            while (!stop && apply_q.empty())
                pthread_cond_wait(&apply_cv, &apply_mtx);

            if (stop && apply_q.empty()) {
                pthread_mutex_unlock(&apply_mtx);
                return;
            }

            ar = apply_q.front();
            apply_q.pop();
            pthread_mutex_unlock(&apply_mtx);

            //single-writer index update
            ht.apply(store, ar.seg_idx, ar.obj_idx);
        }
    }

    void submit_put(const std::string& k,
                    const std::vector<uint8_t>& v) {
        pthread_mutex_lock(&req_mtx);
        req_q.push({OpType::PUT, k, v});
        pthread_mutex_unlock(&req_mtx);
        pthread_cond_signal(&req_cv);
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

        // Wake everyone
        pthread_cond_broadcast(&req_cv);
        pthread_cond_broadcast(&apply_cv);

        // Join workers
        for (auto& t : workers)
            pthread_join(t, nullptr);

        // Join apply thread
        pthread_join(apply_thread, nullptr);

        // close(server_sock);
    }

    // bool replicate_object(const ApplyRecord& ar) {

    //     size_t ack = 1; // leader counts

    //     Segment* leader_seg = store.segments[ar.seg_idx];

    //     ObjectEntry obj = leader_seg->entries[ar.obj_idx];

    //     for (size_t i = 0; i < followers.size(); ++i) {

    //         Segment* fseg = followers[i]->store.segments[ar.seg_idx];

    //         uint64_t tail = fseg->meta.tail_idx.load(std::memory_order_acquire);

    //         // copy object
    //         fseg->entries[tail] = obj;

    //         // publish
    //         fseg->meta.tail_idx.store(tail + 1, std::memory_order_release);

    //         ack++;
    //     }

    //     return ack >= quorum;
    // }
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

        size_t ack_count = 1; // count self

        for (int s : peer_socks) {
            std::cout << "Sending replication to follower socket " << s << "...\n";
            if (send_message(s, msg)) {
                uint8_t ack;
                if (recv_all(s, &ack, 1) && ack == (uint8_t)MsgType::ACK) {
                    ack_count++;
                    std::cout << "Received ACK from follower socket " << s << "\n";
                }
            }
        }


        std::cout << " quorum = " << quorum << ", ack_count = " << ack_count << "\n";
        std::cout << "Peers: " << peer_socks.size() << "\n";

        return ack_count >= quorum;
    }


};