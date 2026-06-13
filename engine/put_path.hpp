#pragma once
#include <string>
#include <vector>
#include "../storage/segment_store.hpp"
#include "../concurrency/incarnation.hpp"
#include "../replica/replica_state.hpp"
#include "../engine/worker.hpp"

struct ApplyRecord {
    size_t seg_idx;
    uint64_t obj_idx;
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

            seg_idx = new_seg;

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

        if (idx == UINT64_MAX) {

            seg.seal();
            auto new_seg = store.acquire_free_segment(w.worker_id, term);

            if (new_seg < 0)
                return false;

            w.active_segment.store(new_seg, std::memory_order_release);

            Segment& next = *store.segments[new_seg];

            idx = next.append(obj);

            if (idx == UINT64_MAX)
                return false;

            out_apply = {
                (size_t)new_seg, idx
            };
            return true;
        }

        if (seg.is_full())
            seg.seal();

        out_apply = {
            seg_idx,
            idx
        };

        return true;
    }

    // engine/put_path.hpp
    static bool put_replicated(
        ReplicaState& rs,
        SegmentStore& store,
        const std::string& key,
        const std::vector<uint8_t>& value,
        uint64_t term,
        uint64_t seq,
        uint64_t incarnation,  // ← Use this directly
        ApplyRecord& ar) {

        size_t h = std::hash<std::string>{}(key);
        size_t seg_idx = h % store.segments.size();
        Segment& seg = *store.segments[seg_idx];

        if (seg.meta.status.load(std::memory_order_acquire) != (uint8_t)SegmentStatus::ACTIVE) {
            if (!store.try_acquire(seg, rs.replica_id, term))
                return false;
        }

        // Create object using LEADER'S metadata (no local incarnation!)
        ObjectEntry obj(term, seq, incarnation, key, value);

        uint64_t idx = seg.append(obj);
        if (idx == UINT64_MAX) return false;

        if (seg.is_full()) seg.seal();

        ar.seg_idx = seg_idx;
        ar.obj_idx = idx;
        return true;
    }
};
