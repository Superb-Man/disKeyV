#pragma once
#include <atomic>
#include <cstdint>

struct Worker {
    uint64_t worker_id;
    std::atomic<uint64_t> sequence_number;
    std::atomic<uint64_t> active_segment;

    explicit Worker(uint64_t id)
        : worker_id(id),
          sequence_number(0),
          active_segment(UINT64_MAX) {}
};