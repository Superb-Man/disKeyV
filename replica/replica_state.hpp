#pragma once
#include <atomic>
#include <cstdint>
#include <mutex>

enum class ReplicaRole {
    LEADER,
    FOLLOWER,
    CANDIDATE
};

using Role = ReplicaRole;

struct ReplicaState {
    static constexpr uint64_t kNoReplicaId = UINT64_MAX;

    uint64_t replica_id;                  // Stable identity used for fencing.
    std::atomic<uint64_t> current_term;   // Epoch accepted for replication.
    std::atomic<ReplicaRole> role;        // Current election role.
    std::atomic<uint64_t> leader_id;      // kNoReplicaId when no leader is known.

    // current_term, voted_term, and voted_for must be evaluated together when
    // granting a vote. The mutex prevents two candidate connections from both
    // receiving this replica's vote in the same term.
    std::mutex election_mutex;
    uint64_t voted_term;
    uint64_t voted_for;

    ReplicaState(uint64_t id, ReplicaRole initial_role)
        : replica_id(id),
          current_term(1),
          role(initial_role),
          leader_id(initial_role == ReplicaRole::LEADER ? id : kNoReplicaId),
          voted_term(initial_role == ReplicaRole::LEADER ? 1 : 0),
          voted_for(initial_role == ReplicaRole::LEADER ? id : kNoReplicaId) {}
};
