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

    Replica(uint64_t rid, int nworkers)
        : rs(rid),
          store(32, 1024),
          ht(4096),
          inc(2048),
          stop(false),
          shutdown_called(false) {
        pthread_mutex_init(&req_mtx, nullptr);
        pthread_cond_init(&req_cv, nullptr);

        pthread_mutex_init(&apply_mtx, nullptr);
        pthread_cond_init(&apply_cv, nullptr);

        start_workers(nworkers);
        start_apply_thread();
    }

    ~Replica() {
        shutdown();

        pthread_mutex_destroy(&req_mtx);
        pthread_cond_destroy(&req_cv);

        pthread_mutex_destroy(&apply_mtx);
        pthread_cond_destroy(&apply_cv);
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
                if (PutPath::put(
                        w, rs, store, inc,
                        req.key, req.value, ar)) {

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
};
