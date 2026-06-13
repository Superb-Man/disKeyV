#pragma once
#include <unordered_map>
#include <string>
#include "../storage/segment_store.hpp"
#include <shared_mutex>
#include <atomic>
#include <mutex>

using OffsetType = uint64_t;

class HashTable {
    std::unordered_map<std::string, OffsetType> map;
    mutable std::shared_mutex ht_mutex;

public:
    HashTable(size_t) {}

    void apply(SegmentStore& store, size_t seg_idx, uint64_t obj_idx) {

        std::unique_lock<std::shared_mutex> lock(ht_mutex);

        if (seg_idx >= store.segments.size())
            return;

        Segment* seg = store.segments[seg_idx];

        if (obj_idx >= seg->capacity)
            return;

        std::atomic_thread_fence(std::memory_order_acquire);

        ObjectEntry& obj = seg->entries[obj_idx];

        char safe_key[65];

        std::memcpy(safe_key, obj.key, 64);

        safe_key[64] = '\0';

        bool valid = false;

        for (int i = 0; i < 64; i++) {

            if (safe_key[i] != 0) {
                valid = true;
                break;
            }
        }

        if (!valid)
            return;

        auto it = map.find(obj.key);

        if (it == map.end()) {

            map[obj.key] = ((uint64_t)seg_idx << 32) | obj_idx;

            return;
        }

        auto packed = it->second;

        auto old_seg = packed >> 32;

        auto old_idx = packed & 0xffffffffULL;

        ObjectEntry* old = &store.segments[old_seg]->entries[old_idx];

        if (std::tie(obj.term_id, obj.incarnation)>std::tie(old->term_id, old->incarnation)) {

            map[obj.key] =((uint64_t)seg_idx << 32) | obj_idx;
        }
    }
    
    /**
     * Get the ObjectEntry pointer for a given key.
     * @param store The SegmentStore containing segments.
     * @param k The key to look up.
     * @return Pointer to the ObjectEntry if found, nullptr otherwise.
     */
    ObjectEntry* get(SegmentStore& store, const std::string& key) {
        std::shared_lock<std::shared_mutex>lock(ht_mutex);

        auto it = map.find(key);

        if (it == map.end())
            return nullptr;

        auto packed = it->second;

        auto seg_idx = packed >> 32;

        auto obj_idx = packed & 0xffffffffULL;

        if (seg_idx >= store.segments.size())
            return nullptr;

        if (obj_idx >= store.segments[seg_idx]->capacity)
            return nullptr;

        std::atomic_thread_fence(std::memory_order_acquire);

        auto& obj = store.segments[seg_idx]->entries[obj_idx];

        if (std::string(obj.key, key.size()) != key)
            return nullptr;

        return &obj;
    }
};