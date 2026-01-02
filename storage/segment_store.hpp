#pragma once
#include "segment.hpp"
#include <vector>
#include <atomic>
#include <cstdint>
#include <cstdlib>

struct SegmentStore {
    std::vector<Segment*> segments;
    std::atomic<uint64_t> global_seg_ver;

    SegmentStore(size_t nseg, uint32_t obj_size, uint64_t cap)
        : global_seg_ver(0) {
        segments.reserve(nseg);
        for (size_t i = 0; i < nseg; i++) {
            segments.push_back(static_cast<Segment*>(malloc(sizeof(Segment))));
        }
    }

    ~SegmentStore() {
        for (Segment* s : segments) {
            free(s);
        }
    }

    Segment& select(uint64_t hash) {
        return *segments[hash % segments.size()];
    }

    bool try_acquire(Segment& seg, uint64_t tid, uint64_t term) {
        uint64_t expected = UINT64_MAX;
        if (!seg.meta.owner_id.compare_exchange_strong(expected, tid))
            return false;

        seg.meta.seg_ver.store(global_seg_ver.fetch_add(1) + 1, std::memory_order_release);
        seg.meta.term_id.store(term, std::memory_order_release);
        seg.meta.status.store((uint8_t)SegmentStatus::ACTIVE, std::memory_order_release);
        seg.meta.tail_idx.store(0, std::memory_order_release);

        return true;
    }
};
