#pragma once
#include <unordered_map>
#include <string>
#include "../storage/segment_store.hpp"

using OffsetType = uint64_t;

class HashTable {
    std::unordered_map<std::string, OffsetType> map;

public:
    HashTable(size_t) {}

    void apply(SegmentStore& store, size_t seg_idx, uint64_t obj_idx) {
        if (seg_idx >= store.segments.size()) return;
            Segment* seg = store.segments[seg_idx];
            if (obj_idx >= seg->capacity) return;

            // Acquire fence to see latest writes
            std::atomic_thread_fence(std::memory_order_acquire);
            
            ObjectEntry& obj = seg->entries[obj_idx];

            // validate key before use
            char safe_key[65];
            std::memcpy(safe_key, obj.key, 64);
            safe_key[64] = '\0';
            
            // Skip if key is all zeros (uninitialized)
            bool is_valid = false;
            for (int i = 0; i < 64; i++) {
                if (safe_key[i] != 0) {
                    is_valid = true;
                    break;
                }
            }
            if (!is_valid) return;

        auto it = map.find(obj.key);
        if (it == map.end()) {
            map[obj.key] = (seg_idx << 32) | obj_idx;
            return;
        }

        
        ObjectEntry* old = &store.segments[it->second >> 32]->entries[it->second & 0xffffffff];

        if (std::tie(obj.term_id, obj.incarnation, obj.seq_num) >
            std::tie(old->term_id, old->incarnation, old->seq_num)) {
            map[obj.key] = (seg_idx << 32) | obj_idx;
        }
    }
    
    /**
     * Get the ObjectEntry pointer for a given key.
     * @param store The SegmentStore containing segments.
     * @param k The key to look up.
     * @return Pointer to the ObjectEntry if found, nullptr otherwise.
     */
    ObjectEntry* get(SegmentStore& store, const std::string& k) {
        auto it = map.find(k);
        if (it == map.end()) return nullptr;

        OffsetType packed = it->second;
        size_t seg_idx = static_cast<size_t>(packed >> 32);
        uint64_t obj_idx = static_cast<uint64_t>(packed & 0xFFFFFFFFULL);

        if (seg_idx >= store.segments.size()) return nullptr;
        if (obj_idx >= store.segments[seg_idx]->capacity) return nullptr;

        std::atomic_thread_fence(std::memory_order_acquire);
        ObjectEntry& obj = store.segments[seg_idx]->entries[obj_idx];
        
        // Validate key matches requested key
        if (std::string(obj.key, k.size()) != k) {
            return nullptr;
        }
        
        return &obj;
    }
};
