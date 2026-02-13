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
    size_t replica_count;
    size_t quorum; // number of replicas required for majority
    std::vector<Follower*> followers;

    Replica(uint64_t rid, int nworkers, size_t total_replicas)
        : rs(rid),
        store(32, 1024),
        ht(4096),
        inc(2048),
        stop(false),
        shutdown_called(false),
        replica_count(total_replicas),
        quorum((total_replicas / 2) + 1)
    {
        pthread_mutex_init(&req_mtx, nullptr);
        pthread_cond_init(&req_cv, nullptr);

        pthread_mutex_init(&apply_mtx, nullptr);
        pthread_cond_init(&apply_cv, nullptr);

        for (size_t i = 1; i < replica_count; ++i) {
            followers.push_back(new Follower(i, 32, 1024));
        }

        start_workers(nworkers);
        start_apply_thread();
    }


    ~Replica() {
        shutdown();

        pthread_mutex_destroy(&req_mtx);
        pthread_cond_destroy(&req_cv);

        pthread_mutex_destroy(&apply_mtx);
        pthread_cond_destroy(&apply_cv);

        for (Follower* f : followers) {
            delete f;
        }

        followers.clear();
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
                    // STEP 1: replicate object
                    if (!replicate_object(ar)) {
                        std::cerr << "Replication failed\n";
                        continue;
                    }

                    // STEP 2: commit locally (after majority)
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
    }

    bool replicate_object(const ApplyRecord& ar) {

        size_t ack = 1; // leader counts

        Segment* leader_seg = store.segments[ar.seg_idx];

        ObjectEntry obj = leader_seg->entries[ar.obj_idx];

        for (size_t i = 0; i < followers.size(); ++i) {

            Segment* fseg = followers[i]->store.segments[ar.seg_idx];

            uint64_t tail = fseg->meta.tail_idx.load(std::memory_order_acquire);

            // copy object
            fseg->entries[tail] = obj;

            // publish
            fseg->meta.tail_idx.store(tail + 1, std::memory_order_release);

            ack++;
        }

        return ack >= quorum;
    }

};