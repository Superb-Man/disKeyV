#pragma once
#include "segment_metadata.hpp"
#include "object_entry.hpp"
#include"segment_metadata.hpp"
#include <vector>
#include <atomic>

struct Segment {
    SegmentMetadata meta;
    std::vector<ObjectEntry> entries; 
    size_t seg_index;
    uint64_t capacity;

    Segment(size_t idx, uint64_t cap)
        : seg_index(idx), capacity(cap), entries(cap) {}

    uint64_t append(ObjectEntry& obj) {
        uint64_t idx = meta.tail_idx.fetch_add(1, std::memory_order_relaxed);
        if (idx >= entries.size()) {
            return UINT64_MAX;
        }
        // write is visible to other threads
        entries[idx] = obj;
        std::atomic_thread_fence(std::memory_order_release);
        return idx;
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
