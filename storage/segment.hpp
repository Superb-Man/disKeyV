#pragma once
#include "segment_metadata.hpp"
#include "object_entry.hpp"
#include <vector>
#include <atomic>

struct Segment {
    SegmentMetadata meta;
    std::vector<std::atomic<ObjectEntry*>> entries;

    Segment(uint32_t obj_size, uint64_t capacity)
        : entries(capacity) {
        meta.object_size = obj_size;
        for (auto& e : entries) e.store(nullptr);
    }

    uint64_t append(ObjectEntry* obj) {
        uint64_t idx = meta.tail_idx.fetch_add(1);
        entries[idx].store(obj, std::memory_order_release);
        return idx;
    }
};
