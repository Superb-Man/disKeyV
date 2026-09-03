#pragma once
#include "segment_metadata.hpp"
#include "object_entry.hpp"
#include <vector>
#include <atomic>
#include <limits>

struct Segment {
    SegmentMetadata meta;
    size_t seg_index;
    uint64_t capacity;
    std::vector<ObjectEntry> entries;

    Segment(size_t idx, uint64_t cap)
        : seg_index(idx), capacity(cap), entries(static_cast<size_t>(cap)) {}

    uint64_t append(const ObjectEntry& obj) {
        uint64_t idx = meta.tail_idx.load(std::memory_order_relaxed);
        while (idx < capacity &&
               !meta.tail_idx.compare_exchange_weak(
                   idx, idx + 1, std::memory_order_acq_rel,
                   std::memory_order_relaxed)) {}
        if (idx >= capacity) return std::numeric_limits<uint64_t>::max();

        entries[idx] = obj;
        std::atomic_thread_fence(std::memory_order_release);
        return idx;
    }

    bool append_at(uint64_t idx, const ObjectEntry& obj) {
        if (idx >= capacity) return false;
        uint64_t expected = idx;
        if (!meta.tail_idx.compare_exchange_strong(
                expected, idx + 1, std::memory_order_acq_rel,
                std::memory_order_relaxed)) {
            return false;
        }
        entries[idx] = obj;
        std::atomic_thread_fence(std::memory_order_release);
        return true;
    }

    /**
     * Seal the segment, marking it as no longer writable.
     *
     */
    void seal() {
        meta.status.store((uint8_t)SegmentStatus::SEALED, std::memory_order_release);
        meta.owner_id.store(UINT64_MAX, std::memory_order_release);
    }

    /**
     * Check if the segment is full.
     */
    bool is_full() const {
        return meta.tail_idx.load(std::memory_order_acquire) >= capacity;
    }
};
