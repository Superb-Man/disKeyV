#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <vector>

#include "../network/message.hpp"
#include "../replica/replica_state.hpp"
#include "../storage/segment_store.hpp"


class SegmentOwnershipQuorum {
public:
    SegmentOwnershipQuorum(const NetMessage& request, size_t follower_count,
                           uint64_t previous_committed_version)
        : request_(request), follower_ids_(follower_count, 0),
          quorum_((follower_count + 1) / 2 + 1) {

        if (request_.type != MsgType::SEGMENT_OWN_REQUEST ||
            !message_detail::valid_shape(request_) ||
            request_.sender_id == ReplicaState::kNoReplicaId ||
            previous_committed_version == std::numeric_limits<uint64_t>::max() ||
            request_.segment_version != previous_committed_version + 1) {
            throw std::invalid_argument("invalid or out-of-order ownership proposal");
        }
    }

    const NetMessage& request() const { return request_; }

    bool observe(size_t peer_index, const NetMessage& reply) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (peer_index >= follower_ids_.size() ||
            reply.type != MsgType::SEGMENT_OWN_REPLY ||
            !message_detail::valid_shape(reply) ||
            reply.sender_id == request_.sender_id ||
            reply.sender_id == ReplicaState::kNoReplicaId) {
            return false;
        }
        highest_observed_term_ = std::max(highest_observed_term_, reply.term);
        if (reply.status != OperationStatus::OK || reply.term != request_.term ||
            reply.worker_id != request_.worker_id ||
            reply.segment_index != request_.segment_index ||
            reply.segment_term != request_.segment_term ||
            reply.segment_version != request_.segment_version ||
            follower_ids_[peer_index] != 0 ||
            std::find(follower_ids_.begin(), follower_ids_.end(), reply.sender_id) !=
                follower_ids_.end()) {
            return false;
        }
        follower_ids_[peer_index] = reply.sender_id;
        ++accepted_followers_;
        return true;
    }

    // Recheck under the election lock: enough old-term ACKs cannot authorize a
    // fenced leader
    OperationStatus committed_status(ReplicaState& state, const SegmentStore& store) {
        std::lock_guard<std::mutex> lock(mutex_);
        std::lock_guard<std::mutex> election_lock(state.election_mutex);
        if (state.replica_id != request_.sender_id ||
            state.role.load(std::memory_order_acquire) != Role::LEADER ||
            state.current_term.load(std::memory_order_acquire) != request_.term ||
            highest_observed_term_ > request_.term) {
            return OperationStatus::NOT_LEADER;
        }
        if (request_.segment_index >= store.segments.size()) {
            return OperationStatus::STORAGE_ERROR;
        }
        const Segment& segment = *store.segments[request_.segment_index];
        if (segment.meta.status.load(std::memory_order_acquire) !=
                static_cast<uint8_t>(SegmentStatus::ACTIVE) ||
            segment.meta.owner_id.load(std::memory_order_acquire) != request_.worker_id ||
            segment.meta.term_id.load(std::memory_order_acquire) != request_.segment_term ||
            segment.meta.seg_ver.load(std::memory_order_acquire) != request_.segment_version ||
            segment.meta.tail_idx.load(std::memory_order_acquire) != 0) {
            return OperationStatus::STORAGE_ERROR;
        }

        return accepted_followers_ + 1 >= quorum_ ? OperationStatus::OK
                                                : OperationStatus::NO_QUORUM;
    }

private:
    const NetMessage request_; // Only the small ownership descriptor; no objects.
    std::vector<uint64_t> follower_ids_; // Zero means no matching ACK from this slot.
    const size_t quorum_;
    std::mutex mutex_;
    size_t accepted_followers_{0};
    uint64_t highest_observed_term_{0}; // Blocks commit after a higher-term reply.
};
