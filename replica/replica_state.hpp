#pragma once
#include <atomic>
#include <cstdint>

enum class ReplicaRole {
    LEADER,
    FOLLOWER
};

using Role = ReplicaRole;

struct ReplicaState {
    uint64_t replica_id;                  // Stable identity used for fencing.
    std::atomic<uint64_t> current_term;   // Epoch accepted for replication.
    std::atomic<ReplicaRole> role;        // Static role in the current design.

    ReplicaState(uint64_t id, ReplicaRole initial_role)
        : replica_id(id), current_term(1), role(initial_role) {}
};
