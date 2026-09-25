#pragma once

#include <condition_variable>
#include <functional>
#include <memory>
#include <thread>
#include "segment_ownership.hpp"
#include "../network/endpoint.hpp"
#include "../storage/segment_gc.hpp"


class OwnershipReplication {
public:
    using Capture = std::function<SegmentState(uint64_t)>;
    OwnershipReplication(ReplicaState& state, const SegmentStore& store,
                         const std::vector<PeerEndpoint>& peers, Capture capture,
                         std::function<void(uint64_t)> higher_term)
        : state_(state), store_(store), peers_(peers), capture_(std::move(capture)),
          higher_term_(std::move(higher_term)) {
        try {
            for (size_t i = 0; i < peers_.size(); ++i)
                threads_.emplace_back([this, i] { run(i); });
        } catch (...) {
            stop();
            throw;
        }
    }
    ~OwnershipReplication() { stop(); }

    OperationStatus commit(const NetMessage& request) {
        auto round = std::make_shared<Round>(request, peers_.size());
        {
            std::lock_guard<std::mutex> lock(mutex_);
            current_ = round;
        }
        work_.notify_all();
        {
            std::unique_lock<std::mutex> lock(round->mutex);
            round->changed.wait(lock, [&] {
                return round->accepted + 1 >= (peers_.size() + 1) / 2 + 1 ||
                       round->completed == peers_.size() || round->fenced;
            });
        }
        return round->quorum.committed_status(state_, store_);
    }

private:
    struct Round {
        Round(const NetMessage& request, size_t peers)
            : quorum(request, peers, request.segment_version - 1) {}
        SegmentOwnershipQuorum quorum;
        std::mutex mutex;
        std::condition_variable changed;
        size_t accepted{0};
        size_t completed{0};
        bool fenced{false};
    };

    ReplicaState& state_;
    const SegmentStore& store_;
    const std::vector<PeerEndpoint> peers_;
    Capture capture_;
    std::function<void(uint64_t)> higher_term_;
    std::mutex mutex_;
    std::condition_variable work_;
    std::shared_ptr<Round> current_;
    bool stopping_{false};
    std::vector<std::thread> threads_;

    void stop() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
        }
        work_.notify_all();
        for (auto& thread : threads_) if (thread.joinable()) thread.join();
    }

    void run(size_t peer) {
        int socket = -1;
        auto disconnect = [&] {
            if (socket >= 0) { ::shutdown(socket, SHUT_RDWR); close(socket); socket = -1; }
        };
        std::shared_ptr<Round> previous;
        while (true) {
            std::shared_ptr<Round> round;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                work_.wait(lock, [&] { return stopping_ || current_ != previous; });
                if (stopping_) break;
                round = current_;
            }
            NetMessage request = round->quorum.request();
            NetMessage reply;
            bool received = false;
            try {
                for (int attempt = 0; attempt < 2 && !received; ++attempt) {
                    if (socket < 0) socket = connect_to(peers_[peer].host, peers_[peer].port);
                    if (socket < 0) break;
                    received = send_message(socket, request) && recv_message(socket, reply);
                    if (received && reply.type == MsgType::SEGMENT_OWN_REPLY &&
                        reply.term == request.term &&
                        (reply.status == OperationStatus::OUT_OF_ORDER ||
                         reply.status == OperationStatus::STORAGE_ERROR)) {

                        received = transfer_segment_state(socket, request.term, request.sender_id, [&] {
                            auto state = capture_(request.term);
                            request.layout_version = state.layout;
                            return state;
                        }) && send_message(socket, request) && recv_message(socket, reply);
                    }
                    if (!received) disconnect();
                }
            } catch (...) { disconnect(); }
            const bool accepted = received && round->quorum.observe(peer, reply);
            const bool fenced = received && reply.term > request.term;
            if (fenced) higher_term_(reply.term);
            {
                std::lock_guard<std::mutex> lock(round->mutex);
                if (accepted) ++round->accepted;
                ++round->completed;
                round->fenced = round->fenced || fenced;
            }
            round->changed.notify_all();
            previous = std::move(round);
        }
        disconnect();
    }
};
