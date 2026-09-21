#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "../storage/segment_store.hpp"

using OffsetType = uint64_t;

enum class IndexApplyResult {
    APPLIED,
    SUPERSEDED,
    FULL,
    INVALID
};

inline bool index_apply_succeeded(IndexApplyResult result) {
    return result == IndexApplyResult::APPLIED ||
           result == IndexApplyResult::SUPERSEDED;
}

class HashTable {
public:
    explicit HashTable(size_t capacity) : slots_(capacity) {
        if (capacity == 0) {
            throw std::invalid_argument("hash-table capacity must be nonzero");
        }
        for (auto& slot : slots_) slot.store(kEmpty, std::memory_order_relaxed);
    }

    HashTable(const HashTable&) = delete;
    HashTable& operator=(const HashTable&) = delete;

    // Publish an immutable segment entry into the key index. Concurrent
    // publishers race only on atomic offset slots; objects are never copied
    // into the table itself.
    IndexApplyResult apply(SegmentStore& store, size_t segment_index,
                           uint64_t object_index) {
        OffsetType new_offset = kEmpty;
        if (!pack_offset(segment_index, object_index, new_offset)) {
            return IndexApplyResult::INVALID;
        }

        ObjectEntry* incoming = resolve(store, new_offset);
        if (incoming == nullptr || !valid_key(*incoming)) {
            return IndexApplyResult::INVALID;
        }
        const std::string key(incoming->key);
        const size_t first_slot = std::hash<std::string>{}(key) % slots_.size();

        for (size_t probe = 0; probe < slots_.size(); ++probe) {
            std::atomic<OffsetType>& slot = slots_[(first_slot + probe) % slots_.size()];

            while (true) {
                OffsetType current = slot.load(std::memory_order_acquire);
                if (current == kEmpty) {
                    OffsetType expected = kEmpty;
                    // Winning this CAS establishes the first index location
                    // for the key; a loser reloads and re-evaluates the slot.
                    if (slot.compare_exchange_weak(
                            expected, new_offset, std::memory_order_release,
                            std::memory_order_acquire)) {
                        return IndexApplyResult::APPLIED;
                    }
                    continue;
                }

                ObjectEntry* existing = resolve(store, current);
                if (existing == nullptr || !valid_key(*existing)) {
                    return IndexApplyResult::INVALID;
                }
                if (!key_equals(*existing, key)) break;

                // An older immutable version remains in SegmentStore but must
                // not replace the offset of a newer visible version.
                if (!newer_than(*incoming, *existing)) {
                    return IndexApplyResult::SUPERSEDED;
                }
                if (slot.compare_exchange_weak(
                        current, new_offset, std::memory_order_acq_rel,
                        std::memory_order_acquire)) {
                    return IndexApplyResult::APPLIED;
                }
            }
        }
        return IndexApplyResult::FULL;
    }

    // GC moves a live object without creating a new version. The caller must
    // finish copying the object before calling this, and keep both locations
    // stable throughout the call. Success changes only the index pointer;
    // readers and replication/recovery may still require the old segment.
    bool relocate(SegmentStore& store, size_t old_segment_index,
                  uint64_t old_object_index, size_t new_segment_index,
                  uint64_t new_object_index) {
        OffsetType old_offset = kEmpty;
        OffsetType new_offset = kEmpty;
        if (!pack_offset(old_segment_index, old_object_index, old_offset) ||
            !pack_offset(new_segment_index, new_object_index, new_offset) ||
            old_offset == new_offset) {
            return false;
        }

        const ObjectEntry* original = resolve(store, old_offset);
        const ObjectEntry* moved = resolve(store, new_offset);
        if (original == nullptr || moved == nullptr ||
            !valid_key(*original) || !valid_key(*moved)) {
            return false;
        }
        const std::string key(original->key);
        if (!key_equals(*moved, key) ||
            original->term_id != moved->term_id ||
            original->seq_num != moved->seq_num ||
            original->incarnation != moved->incarnation ||
            original->value != moved->value) {
            return false;
        }

        const size_t first_slot = std::hash<std::string>{}(key) % slots_.size();
        for (size_t probe = 0; probe < slots_.size(); ++probe) {
            std::atomic<OffsetType>& slot =
                slots_[(first_slot + probe) % slots_.size()];
            OffsetType current = slot.load(std::memory_order_acquire);
            if (current == kEmpty) return false;
            if (current == old_offset) {
                // If a writer published a newer version while GC copied, leave
                // that version untouched. A strong CAS needs no retry loop.
                return slot.compare_exchange_strong(
                    current, new_offset, std::memory_order_acq_rel,
                    std::memory_order_acquire);
            }
            const ObjectEntry* existing = resolve(store, current);
            if (existing == nullptr || !valid_key(*existing) ||
                key_equals(*existing, key)) {
                return false;
            }
        }
        return false;
    }

    // Follow the same linear-probing chain used by apply. An empty slot ends
    // the search because entries are never deleted from the current index.
    ObjectEntry* get(SegmentStore& store, const std::string& key) const {
        const size_t first_slot = std::hash<std::string>{}(key) % slots_.size();
        for (size_t probe = 0; probe < slots_.size(); ++probe) {
            const OffsetType current =
                slots_[(first_slot + probe) % slots_.size()].load(
                    std::memory_order_acquire);
            if (current == kEmpty) return nullptr;

            ObjectEntry* entry = resolve(store, current);
            if (entry == nullptr || !valid_key(*entry)) return nullptr;
            if (key_equals(*entry, key)) return entry;
        }
        return nullptr;
    }

    size_t capacity() const { return slots_.size(); }

    void swap_contents(HashTable& other) { slots_.swap(other.slots_); }

private:
    static constexpr OffsetType kEmpty = 0; // Reserved "no object" sentinel.
    static constexpr uint64_t kLowMask = 0xffffffffULL;
    // Each slot packs segment index in the high half and object index low.
    std::vector<std::atomic<OffsetType>> slots_;

    // Segment indices are biased by one so packed offset zero stays available
    // as the empty sentinel while physical segment zero remains representable.
    static bool pack_offset(size_t segment_index, uint64_t object_index,
                            OffsetType& packed) {
        if (segment_index >= std::numeric_limits<uint32_t>::max() ||
            object_index > std::numeric_limits<uint32_t>::max()) {
            return false;
        }
        const uint64_t biased_segment = static_cast<uint64_t>(segment_index) + 1;
        packed = (biased_segment << 32U) | object_index;
        return true;
    }

    static bool unpack_offset(OffsetType packed, size_t& segment_index,
                              uint64_t& object_index) {
        const uint64_t biased_segment = packed >> 32U;
        if (biased_segment == 0) return false;
        segment_index = static_cast<size_t>(biased_segment - 1);
        object_index = packed & kLowMask;
        return true;
    }

    static ObjectEntry* resolve(SegmentStore& store, OffsetType packed) {
        size_t segment_index = 0;
        uint64_t object_index = 0;
        if (!unpack_offset(packed, segment_index, object_index) ||
            segment_index >= store.segments.size()) {
            return nullptr;
        }
        Segment* segment = store.segments[segment_index];
        if (object_index >= segment->capacity ||
            object_index >= segment->meta.tail_idx.load(std::memory_order_acquire)) {
            return nullptr;
        }
        return &segment->entries[object_index];
    }

    static bool valid_key(const ObjectEntry& entry) {
        return entry.key[0] != '\0' &&
               std::memchr(entry.key, '\0', sizeof(entry.key)) != nullptr;
    }

    static bool key_equals(const ObjectEntry& entry, const std::string& key) {
        const size_t stored_length = strnlen(entry.key, sizeof(entry.key));
        return stored_length == key.size() &&
               std::memcmp(entry.key, key.data(), stored_length) == 0;
    }

    static bool newer_than(const ObjectEntry& incoming,
                           const ObjectEntry& existing) {
        return incoming.term_id > existing.term_id ||
               (incoming.term_id == existing.term_id &&
                incoming.incarnation > existing.incarnation);
    }
};
