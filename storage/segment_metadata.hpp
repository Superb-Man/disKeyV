#pragma once
#include <atomic>
#include <cstdint>

enum class SegmentStatus : uint8_t {
    FREE,
    ACTIVE,
    SEALED
};

struct SegmentMetadata {
    std::atomic<uint64_t> owner_id;
    std::atomic<uint64_t> term_id;
    std::atomic<uint8_t> status;
    std::atomic<uint64_t> seg_ver;
    uint32_t object_size;
    std::atomic<uint64_t> tail_idx;
    std::atomic<bool> committed{false};
    
    SegmentMetadata()
        : owner_id(UINT64_MAX),
          term_id(0),
          status((uint8_t)SegmentStatus::FREE),
          seg_ver(0),
          object_size(0),
          tail_idx(0) {}
};
