#pragma once
#include <atomic>
#include <cstdint>

struct Worker {
    uint64_t worker_id;                       // Stable replication-stream ID.
    std::atomic<uint64_t> sequence_number;    // Last sequence issued in stream.
    std::atomic<uint64_t> active_segment;     // UINT64_MAX means no segment.

    explicit Worker(uint64_t id)
        : worker_id(id),
          sequence_number(0),
          active_segment(UINT64_MAX) {}
};
