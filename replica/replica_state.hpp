#pragma once
#include <atomic>
#include <cstdint>

enum class ReplicaRole {
    LEADER,
    FOLLOWER
};

struct ReplicaState {
    uint64_t replica_id;
    std::atomic<uint64_t> current_term;
    std::atomic<ReplicaRole> role;

    ReplicaState(uint64_t id)
        : replica_id(id), current_term(1), role(ReplicaRole::LEADER) {}
};
