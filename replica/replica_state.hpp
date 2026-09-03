#pragma once
#include <atomic>
#include <cstdint>

enum class ReplicaRole {
    LEADER,
    FOLLOWER
};

using Role = ReplicaRole;

struct ReplicaState {
    uint64_t replica_id;
    std::atomic<uint64_t> current_term;
    std::atomic<ReplicaRole> role;

    ReplicaState(uint64_t id, ReplicaRole initial_role)
        : replica_id(id), current_term(1), role(initial_role) {}
};
