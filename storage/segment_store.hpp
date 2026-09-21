#pragma once
#include "segment.hpp"
#include "../logging/log.hpp"
#include <vector>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <array>
#include <mutex>

struct SegmentStore {
    std::vector<Segment*> segments;          // Fixed physical segment arena.
    std::atomic<uint64_t> global_seg_ver{0}; // Monotonic allocation generation.
    mutable std::mutex data_mutex;
    std::atomic<uint64_t> layout_version{0}; // Changes when physical offsets move.
    std::array<std::pair<uint64_t, uint64_t>, 64> progress{};
    std::vector<size_t> pending; // Unpublished objects pin their source segment.
    std::atomic<uint64_t> reclaimed_segments{0};

    SegmentStore(size_t nseg, uint64_t cap) : pending(nseg, 0) {
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


    bool release_sealed_segment(size_t segment_index, uint64_t expected_owner,
                                uint64_t expected_term,
                                uint64_t expected_version) {
        if (segment_index >= segments.size() || expected_owner == UINT64_MAX) {
            return false;
        }
        Segment& segment = *segments[segment_index];
        if (segment.meta.status.load(std::memory_order_acquire) != static_cast<uint8_t>(SegmentStatus::SEALED) ||
            segment.meta.owner_id.load(std::memory_order_acquire) != expected_owner ||
            segment.meta.term_id.load(std::memory_order_acquire) != expected_term ||
            segment.meta.seg_ver.load(std::memory_order_acquire) != expected_version) {
            return false;
        }
        const uint64_t tail = segment.meta.tail_idx.load(std::memory_order_acquire);
        if (tail > segment.capacity || tail > segment.entries.size()) return false;

        for (uint64_t offset = 0; offset < tail; ++offset) {
            ObjectEntry& object = segment.entries[offset];
            // clear() keeps vector capacity: swap releases the old payload
            // allocation while retaining the preallocated object-slot array.
            std::vector<uint8_t>().swap(object.value);
            object = ObjectEntry{};
        }
        segment.meta.tail_idx.store(0, std::memory_order_relaxed);
        segment.meta.term_id.store(0, std::memory_order_relaxed);
        segment.meta.committed.store(false, std::memory_order_relaxed);
        segment.meta.object_size = 0;
        segment.meta.owner_id.store(UINT64_MAX, std::memory_order_relaxed);
        
        // Keep seg_ver until allocation assigns a fresh generation. Publish
        // FREE last so no allocator can claim a partially cleared segment.
        segment.meta.status.store(static_cast<uint8_t>(SegmentStatus::FREE),
                                  std::memory_order_release);
        DISKEYV_DEBUG("GC", "segment=" << segment_index
                                       << " state=released worker=" << expected_owner
                                       << " term=" << expected_term
                                       << " version=" << expected_version
                                       << " objects=" << tail);
        return true;
    }

    /**
     * @param seg The segment to acquire.
     * @param wid The worker ID attempting to acquire the segment.
     * @param term The term ID for the acquisition.
     * @return True if acquisition was successful, false otherwise.
     */
    bool try_acquire(Segment& seg, uint64_t wid, uint64_t term,
                     uint64_t replicated_version = 0) {
        uint8_t status = seg.meta.status.load(std::memory_order_acquire);

        if (status != (uint8_t)SegmentStatus::FREE)
            return false;

        uint64_t expected_owner = UINT64_MAX;

        if (!seg.meta.owner_id.compare_exchange_strong(expected_owner, wid, std::memory_order_acq_rel))
            return false;

        uint64_t version = replicated_version;
        if (version == 0) version = global_seg_ver.fetch_add(1) + 1;
        else {
            // Reproduce a leader allocation, not a second local allocation.
            uint64_t current = global_seg_ver.load();
            while (current < version && !global_seg_ver.compare_exchange_weak(current, version)) {}
        }
        seg.meta.seg_ver.store(version, std::memory_order_release);

        seg.meta.term_id.store(term, std::memory_order_release);

        seg.meta.tail_idx.store(0, std::memory_order_release);
        seg.meta.status.store((uint8_t)SegmentStatus::ACTIVE, std::memory_order_release);

        DISKEYV_DEBUG("SEGMENT",
                      "segment=" << seg.seg_index << " state=active worker="
                                 << wid << " term=" << term << " version="
                                 << seg.meta.seg_ver.load(
                                        std::memory_order_acquire));

        return true;
    }

    // Claim any FREE segment for one worker/term. The status CAS is the
    // ownership arbitration point when multiple workers allocate concurrently.
    int64_t acquire_free_segment(uint64_t wid, uint64_t term) {

        for (size_t i = 0; i < segments.size(); i++) {

            Segment& seg = *segments[i];

            uint8_t expected =
                (uint8_t)SegmentStatus::FREE;

            if (!seg.meta.status.compare_exchange_strong(
                    expected,
                    (uint8_t)SegmentStatus::ACTIVE,
                    std::memory_order_acq_rel))
                continue;

            seg.meta.owner_id.store(wid, std::memory_order_release);
            seg.meta.term_id.store(term, std::memory_order_release);
            seg.meta.seg_ver.store(global_seg_ver.fetch_add(1) + 1, std::memory_order_release);
            seg.meta.tail_idx.store(0, std::memory_order_release);

            DISKEYV_DEBUG("SEGMENT",
                          "segment=" << i << " state=active worker=" << wid
                                     << " term=" << term << " version="
                                     << seg.meta.seg_ver.load(
                                            std::memory_order_acquire));

            return static_cast<int64_t>(i);
        }

        DISKEYV_WARN("SEGMENT",
                     "state=allocation-failed worker=" << wid
                                                        << " term=" << term
                                                        << " segments="
                                                        << segments.size());
        return -1;
    }
};
