#pragma once
#include "segment.hpp"
#include <vector>
#include <atomic>
#include <cstdint>
#include <cstdlib>

struct SegmentStore {
    std::vector<Segment*> segments;
    std::atomic<uint64_t> global_seg_ver{0};

    SegmentStore(size_t nseg, uint64_t cap) {
        for (size_t i = 0; i < nseg; i++) {
            segments.push_back(new Segment(i, cap));
        }
    }

    ~SegmentStore() {
        for (Segment* s : segments) {
            delete s;
        }
    }

    Segment& select(uint64_t hash) {
        return *segments[hash % segments.size()];
    }

    /**
     * @param seg The segment to acquire.
     * @param wid The worker ID attempting to acquire the segment.
     * @param term The term ID for the acquisition.
     * @return True if acquisition was successful, false otherwise.
     */
    bool try_acquire(Segment& seg, uint64_t wid, uint64_t term) {
        uint8_t status = seg.meta.status.load(std::memory_order_acquire);
        if (status != (uint8_t)SegmentStatus::FREE)
            return false;

        uint64_t expected_owner = UINT64_MAX;
        if (!seg.meta.owner_id.compare_exchange_strong(
            expected_owner, wid, 
            std::memory_order_acq_rel)) {
            return false;
        }

        seg.meta.seg_ver.store(++global_seg_ver, std::memory_order_release);
        seg.meta.term_id.store(term, std::memory_order_release);
        seg.meta.status.store((uint8_t)SegmentStatus::ACTIVE, std::memory_order_release);
        seg.meta.tail_idx.store(0, std::memory_order_release);

        return true;
    }
};
