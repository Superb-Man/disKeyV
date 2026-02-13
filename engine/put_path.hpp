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
        const std::vector<uint8_t>& value, ApplyRecord& out_apply) {

            uint64_t seq = w.sequence_number.fetch_add(1, std::memory_order_relaxed) + 1;
            uint64_t incarnation = inc.next(key);
            uint64_t term = rs.current_term.load(std::memory_order_acquire);

            size_t h = std::hash<std::string>{}(key);
            size_t seg_idx = h % store.segments.size();
            Segment& seg = *store.segments[seg_idx];

            if (seg.meta.status.load(std::memory_order_acquire)
                != (uint8_t)SegmentStatus::ACTIVE) {
                if (!store.try_acquire(seg, w.worker_id, term))
                    return false;
        }

        ObjectEntry obj(term, seq, incarnation, key, value);
        uint64_t idx = seg.append(obj);
        if (idx == UINT64_MAX)
            return false;

        if (seg.is_full())
            seg.seal();

        //produce apply record
        out_apply = { seg_idx, idx };
        return true;
    }
};
