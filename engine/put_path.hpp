#pragma once
#include <string>
#include <vector>
#include "../storage/segment_store.hpp"
#include "../concurrency/incarnation.hpp"
#include "../replica/replica_state.hpp"
#include "../engine/worker.hpp"
#include <limits>

struct ApplyRecord {
    size_t seg_idx;
    uint64_t obj_idx;
    uint64_t worker_id;
};

struct PutPath {
    static bool put(
        Worker& w,
        ReplicaState& rs,
        SegmentStore& store,
        IncarnationTable& inc,
        const std::string& key,
        const std::vector<uint8_t>& value,
        ApplyRecord& out_apply) {

        auto seq = w.sequence_number.fetch_add(1) + 1;

        auto incarnation = inc.next(key);
        auto term = rs.current_term.load(std::memory_order_acquire);

        auto seg_idx = w.active_segment.load(std::memory_order_acquire);

        if (seg_idx == UINT64_MAX) {

            auto new_seg = store.acquire_free_segment(w.worker_id, term);

            if (new_seg < 0)
                return false;

            seg_idx = static_cast<uint64_t>(new_seg);

            w.active_segment.store(seg_idx, std::memory_order_release);
        }

        Segment& seg = *store.segments[seg_idx];

        ObjectEntry obj(
            term,
            seq,
            incarnation,
            key,
            value);

        auto idx = seg.append(obj);

        if (idx == std::numeric_limits<uint64_t>::max()) {

            seg.seal();
            auto new_seg = store.acquire_free_segment(w.worker_id, term);

            if (new_seg < 0)
                return false;

            w.active_segment.store(new_seg, std::memory_order_release);

            Segment& next = *store.segments[new_seg];

            idx = next.append(obj);

            if (idx == std::numeric_limits<uint64_t>::max())
                return false;

            out_apply = {
                static_cast<size_t>(new_seg), idx, w.worker_id
            };
            return true;
        }

        if (seg.is_full())
            seg.seal();

        out_apply = {
            static_cast<size_t>(seg_idx),
            idx,
            w.worker_id
        };

        return true;
    }

    static bool put_replicated_at(
        SegmentStore& store,
        uint64_t worker_id,
        size_t seg_idx,
        uint64_t obj_idx,
        const std::string& key,
        const std::vector<uint8_t>& value,
        uint64_t term,
        uint64_t seq,
        uint64_t incarnation,  // ← Use this directly
        ApplyRecord& ar) {

        if (seg_idx >= store.segments.size()) return false;
        Segment& seg = *store.segments[seg_idx];

        if (seg.meta.status.load(std::memory_order_acquire) !=
            static_cast<uint8_t>(SegmentStatus::ACTIVE)) {
            if (!store.try_acquire(seg, worker_id, term))
                return false;
        } else if (seg.meta.owner_id.load(std::memory_order_acquire) != worker_id ||
                   seg.meta.term_id.load(std::memory_order_acquire) != term) {
            return false;
        }

        ObjectEntry obj(term, seq, incarnation, key, value);
        if (!seg.append_at(obj_idx, obj)) return false;

        if (seg.is_full()) seg.seal();

        ar.seg_idx = seg_idx;
        ar.obj_idx = obj_idx;
        ar.worker_id = worker_id;
        return true;
    }
};
