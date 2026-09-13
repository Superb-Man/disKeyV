#pragma once
#include <atomic>
#include <cstdint>

enum class SegmentStatus : uint8_t {
    FREE,
    ACTIVE,
    SEALED
};

struct SegmentMetadata {
    std::atomic<uint64_t> owner_id;  // Worker allowed to append this segment.
    std::atomic<uint64_t> term_id;   // Epoch in which ownership was assigned.
    std::atomic<uint8_t> status;     // FREE -> ACTIVE -> SEALED lifecycle.
    std::atomic<uint64_t> seg_ver;   // Store-wide allocation generation.
    uint32_t object_size;            // Reserved for fixed-size layout metadata.
    std::atomic<uint64_t> tail_idx;  // First unreserved object position.
    std::atomic<bool> committed{false};  // Reserved segment commit marker.
    
    SegmentMetadata()
        : owner_id(UINT64_MAX),
          term_id(0),
          status((uint8_t)SegmentStatus::FREE),
          seg_ver(0),
          object_size(0),
          tail_idx(0) {}
};
