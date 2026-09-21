#pragma once
#include <string>
#include <vector>
#include "../storage/segment_store.hpp"
#include "../concurrency/incarnation.hpp"
#include "../replica/replica_state.hpp"
#include "../engine/worker.hpp"
#include <limits>

struct ApplyRecord {
    size_t seg_idx;       // Physical segment containing the appended object.
    uint64_t obj_idx;     // Object position inside that segment.
    uint64_t worker_id;   // Logical stream that owns the object.
};

struct PutPath {
    // Append a leader-generated immutable object. Publication to the hash
    // index happens later, after replication reaches the required quorum.
    static bool put(
        Worker& w,
        ReplicaState& rs,
        SegmentStore& store,
        IncarnationTable& inc,
        const std::string& key,
        const std::vector<uint8_t>& value,
        ApplyRecord& out_apply) {

        auto term = rs.current_term.load(std::memory_order_acquire);
        auto seg_idx = w.active_segment.load(std::memory_order_acquire);

        if (seg_idx != UINT64_MAX) {
            if (seg_idx >= store.segments.size()) return false;
            Segment& active = *store.segments[seg_idx];
            // A worker never appends through a stale or exhausted segment
            // handle; sealing makes the transition explicit for recovery.
            if (active.meta.status.load(std::memory_order_acquire) != static_cast<uint8_t>(SegmentStatus::ACTIVE) ||
                active.meta.owner_id.load() != w.worker_id ||
                active.meta.term_id.load() != term ||
                active.is_full()) {
                seg_idx = UINT64_MAX;
                w.active_segment.store(UINT64_MAX, std::memory_order_release);
            }
        }

        if (seg_idx == UINT64_MAX) {
            // GC may have left a partially filled relocation destination.
            int64_t new_seg = -1;
            for (const Segment* candidate : store.segments) {
                if (candidate->meta.status.load() == static_cast<uint8_t>(SegmentStatus::ACTIVE) &&
                    candidate->meta.owner_id.load() == w.worker_id &&
                    candidate->meta.term_id.load() == term && !candidate->is_full()) {
                    new_seg = static_cast<int64_t>(candidate->seg_index);
                    break;
                }
            }
            if (new_seg < 0) new_seg = store.acquire_free_segment(w.worker_id, term);

            if (new_seg < 0)
                return false;

            seg_idx = static_cast<uint64_t>(new_seg);

            w.active_segment.store(seg_idx, std::memory_order_release);
        }

        return put_owned(w, rs, store, inc, term,
                         store.segments[seg_idx]->meta.seg_ver.load(std::memory_order_acquire),
                         key, value, out_apply);
    }

    static bool put_owned(
        Worker& w,
        ReplicaState& rs,
        SegmentStore& store,
        IncarnationTable& inc,
        uint64_t expected_term,
        uint64_t expected_segment_version,
        const std::string& key,
        const std::vector<uint8_t>& value,
        ApplyRecord& out_apply) {

        const uint64_t seg_idx = w.active_segment.load(std::memory_order_acquire);
        if (seg_idx >= store.segments.size() || expected_term == 0 ||
            expected_segment_version == 0 ||
            rs.role.load(std::memory_order_acquire) != Role::LEADER ||
            rs.current_term.load(std::memory_order_acquire) != expected_term) {
            return false;
        }
        Segment& seg = *store.segments[seg_idx];
        if (seg.meta.status.load(std::memory_order_acquire) != static_cast<uint8_t>(SegmentStatus::ACTIVE) ||
            seg.meta.owner_id.load(std::memory_order_acquire) != w.worker_id ||
            seg.meta.term_id.load(std::memory_order_acquire) != expected_term ||
            seg.meta.seg_ver.load(std::memory_order_acquire) != expected_segment_version ||
            seg.is_full()) {
            return false;
        }

        // Sequence orders this worker's replication stream; incarnation
        // orders competing versions of the same key across worker streams.
        // Rejected/stale allocations consume neither counter.
        const auto seq = w.sequence_number.fetch_add(1) + 1;
        const auto incarnation = inc.next(key);

        ObjectEntry obj(
            expected_term,
            seq,
            incarnation,
            key,
            value);

        auto idx = seg.append(obj);

        if (idx == std::numeric_limits<uint64_t>::max()) return false;

        if (seg.is_full())
            seg.seal();

        out_apply = {
            static_cast<size_t>(seg_idx),
            idx,
            w.worker_id
        };

        return true;
    }

    // Reproduce the leader's exact physical offset on a follower or during
    // recovery. append_at enforces that the stream fills offsets contiguously.
    static bool put_replicated_at(
        SegmentStore& store,
        uint64_t worker_id,
        size_t seg_idx,
        uint64_t obj_idx,
        const std::string& key,
        const std::vector<uint8_t>& value,
        uint64_t term,
        uint64_t seq,
        uint64_t incarnation,
        ApplyRecord& ar,
        bool allow_sealed_recovery = false,
        uint64_t segment_term = 0, uint64_t segment_version = 0) {

        if (seg_idx >= store.segments.size()) return false;
        Segment& seg = *store.segments[seg_idx];

        const uint8_t status = seg.meta.status.load(std::memory_order_acquire);
        if (status == static_cast<uint8_t>(SegmentStatus::FREE)) {
            // First record at this offset claims the segment for its original
            // worker and term; later records must match that ownership.
            if (!store.try_acquire(seg, worker_id, segment_term ? segment_term : term, segment_version))
                return false;
        } else if ((status != static_cast<uint8_t>(SegmentStatus::ACTIVE) &&
                    !(allow_sealed_recovery &&
                      status == static_cast<uint8_t>(SegmentStatus::SEALED))) ||
                   seg.meta.owner_id.load(std::memory_order_acquire) !=
                       worker_id ||
                   seg.meta.term_id.load(std::memory_order_acquire) != (segment_term ? segment_term : term)) {
            return false;
        }
        if (segment_version && seg.meta.seg_ver.load() != segment_version) return false;

        ObjectEntry obj(term, seq, incarnation, key, value);
        if (!seg.append_at(obj_idx, obj)) return false;

        if (seg.is_full()) seg.seal();

        ar.seg_idx = seg_idx;
        ar.obj_idx = obj_idx;
        ar.worker_id = worker_id;
        return true;
    }
};
