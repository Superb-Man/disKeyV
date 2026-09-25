#pragma once

#include "../index/hash_table.hpp"
#include "../network/message.hpp"
#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <tuple>
#include <functional>


struct SegmentState {
    uint64_t layout{0};
    uint64_t allocation_version{0};
    uint64_t allocation_term{0};
    std::vector<SegmentOwnershipWire> ownerships;
    std::vector<ReplicationEntry> records;
    std::vector<WorkerProgressWire> progress;

    // Caller holds data_mutex (and excludes recovery/layout replacement).
    static SegmentState capture(const SegmentStore& store) {
        SegmentState state;
        state.layout = store.layout_version.load();
        state.allocation_version = store.global_seg_ver.load();
        state.allocation_term = store.ownership_term.load();
        state.ownerships = capture_ownerships(store);
        for (size_t worker = 0; worker < store.progress.size(); ++worker) {
            const auto& prefix = store.progress[worker];
            if (prefix.first) state.progress.push_back({worker, prefix.first, prefix.second});
        }
        for (const Segment* segment : store.segments) {
            const uint64_t tail = segment->meta.tail_idx.load();
            for (uint64_t offset = 0; offset < tail; ++offset) {
                const ObjectEntry& object = segment->entries[offset];
                state.records.push_back({object.seq_num, object.incarnation,
                    segment->seg_index, offset, object.key, object.value,
                    object.term_id, segment->meta.owner_id.load(),
                    segment->meta.term_id.load(), segment->meta.seg_ver.load()});
            }
        }
        return state;
    }

    static std::vector<SegmentOwnershipWire> capture_ownerships(const SegmentStore& store) {
        std::vector<SegmentOwnershipWire> result;

        for (const Segment* segment : store.segments) {
            const auto status = static_cast<SegmentStatus>(segment->meta.status.load());
            if (status == SegmentStatus::FREE) continue;
            result.push_back({segment->seg_index, segment->meta.owner_id.load(),
                segment->meta.term_id.load(), segment->meta.seg_ver.load(), status == SegmentStatus::SEALED ? 
                                                                            uint64_t{1} : uint64_t{0}});
        }
        return result;
    }


    bool install(SegmentStore& store, HashTable& index) const {
        if (store.segments.empty()) return false;
        SegmentStore staged(store.segments.size(), store.segments.front()->capacity);

        HashTable staged_index(index.capacity());
        std::set<uint64_t> allocated_slots;
        std::set<uint64_t> allocated_versions;
        for (const auto& ownership : ownerships) {
            if (ownership.segment_index >= staged.segments.size() ||
                ownership.worker_id >= staged.progress.size() || !ownership.term ||
                !ownership.version || ownership.version > allocation_version ||
                ownership.sealed > 1 ||
                !allocated_slots.insert(ownership.segment_index).second ||
                !allocated_versions.insert(ownership.version).second ||
                !staged.try_acquire(*staged.segments[ownership.segment_index],
                    ownership.worker_id, ownership.term, ownership.version)) {
                    
                        return false;
                    
                }
        }
        for (const auto& prefix : progress) {
            if (prefix.worker_id >= staged.progress.size() || !prefix.term ||
                staged.progress[prefix.worker_id].first) return false;
            staged.progress[prefix.worker_id] = {prefix.term, prefix.sequence};
        }
        std::set<std::tuple<uint64_t, uint64_t, uint64_t>> sequences;
        for (const auto& entry : records) {
            if (!message_detail::valid_recovery_entry(entry) ||
                !entry.segment_term || !entry.segment_version ||
                entry.segment_index >= staged.segments.size() ||
                !sequences.emplace(entry.worker_id, entry.term, entry.sequence).second) return false;
            const auto& prefix = staged.progress[entry.worker_id];
            if (std::make_pair(entry.term, entry.sequence) > prefix) return false;
            Segment& segment = *staged.segments[entry.segment_index];
            if (segment.meta.status.load() == static_cast<uint8_t>(SegmentStatus::FREE)) {
                if (!ownerships.empty()) return false;
                segment.meta.owner_id.store(entry.worker_id);
                segment.meta.term_id.store(entry.segment_term);
                segment.meta.seg_ver.store(entry.segment_version);
                segment.meta.status.store(static_cast<uint8_t>(SegmentStatus::ACTIVE));
            }
            if (segment.meta.owner_id.load() != entry.worker_id ||
                segment.meta.term_id.load() != entry.segment_term ||
                segment.meta.seg_ver.load() != entry.segment_version ||
                entry.segment_version > allocation_version ||
                !segment.append_at(entry.object_index, ObjectEntry(entry.term,
                    entry.sequence, entry.incarnation, entry.key, entry.value)) ||
                !index_apply_succeeded(staged_index.apply(staged,
                    entry.segment_index, entry.object_index))) return false;
            if (segment.is_full()) segment.seal();
        }
        for (const auto& ownership : ownerships) {
            if (ownership.sealed) staged.segments[ownership.segment_index]->seal();
        }

        for (size_t i = 0; i < store.segments.size(); ++i) {
            Segment& target = *store.segments[i];
            Segment& source = *staged.segments[i];
            target.entries.swap(source.entries);
            target.meta.owner_id.store(source.meta.owner_id.load());
            target.meta.term_id.store(source.meta.term_id.load());
            target.meta.seg_ver.store(source.meta.seg_ver.load());
            target.meta.tail_idx.store(source.meta.tail_idx.load());
            target.meta.committed.store(false);
            target.meta.status.store(source.meta.status.load());
        }
        index.swap_contents(staged_index);
        store.progress = staged.progress;
        std::fill(store.pending.begin(), store.pending.end(), 0);
        store.global_seg_ver.store(allocation_version);

        uint64_t term = allocation_term;
        for (const Segment* segment : staged.segments) term = std::max(term, segment->meta.term_id.load());
        store.ownership_term.store(term);
        store.layout_version.store(layout);
        
        return true;
    }
};

// BEGIN must be acknowledged BEFORE capture: the receiver has stopped data
// ACKs at that point. Otherwise installing an older copy could erase a write
// it acknowledged between capture and transfer. Batches are bounded on wire.
template <typename Capture>
bool transfer_segment_state(int socket, uint64_t term, uint64_t leader,
                            Capture capture) {
    NetMessage begin;
    begin.type = MsgType::STATE_BEGIN;
    begin.term = term;
    begin.sender_id = leader;
    NetMessage reply;

    if (!send_message(socket, begin) || !recv_message(socket, reply) ||
        reply.type != MsgType::ACK || reply.term != term ||
        reply.status != OperationStatus::OK) return false;
    const SegmentState state = capture();

    for (size_t first = 0; first < state.records.size();) {
        NetMessage batch;
        batch.type = MsgType::STATE_BATCH;
        batch.term = term;
        batch.sender_id = leader;
        size_t bytes = 0;

        while (first < state.records.size() && batch.entries.size() < kMaxWireRecoveryBatchEntries) {
            const auto& entry = state.records[first];
            const size_t size = entry.key.size() + entry.value.size() + 10 * sizeof(uint64_t);
            if (!batch.entries.empty() && bytes + size > kMaxWireRecoveryBatchBytes) break;
            batch.entries.push_back(entry);
            bytes += size;
            ++first;
        }
        if (!send_message(socket, batch) || !recv_message(socket, reply) ||
            reply.type != MsgType::ACK || reply.term != term ||
            reply.status != OperationStatus::OK) return false;
    }

    NetMessage end;
    end.type = MsgType::STATE_END;
    end.term = term;
    end.sender_id = leader;
    end.seq = state.records.size();
    end.layout_version = state.layout;
    end.last_segment_version = state.allocation_version;
    end.last_segment_term = state.allocation_term;
    end.ownerships = state.ownerships;
    end.worker_progress = state.progress;
    return send_message(socket, end) && recv_message(socket, reply) &&
           reply.type == MsgType::ACK && reply.term == term &&
           reply.status == OperationStatus::OK;
}


inline size_t collect_segments(SegmentStore& store, HashTable& index,
                              const std::function<int64_t(uint64_t, uint64_t)>& allocate = {},
                              uint64_t allocation_term = 0) {
    size_t released = 0;
    bool allocation_failed = false;
    for (Segment* source : store.segments) {
        if (source->meta.status.load() != static_cast<uint8_t>(SegmentStatus::SEALED) ||
            store.pending[source->seg_index]) continue;
        std::vector<uint64_t> live;
        const uint64_t tail = source->meta.tail_idx.load();
        
        for (uint64_t offset = 0; offset < tail; ++offset) {
            ObjectEntry& object = source->entries[offset];
            if (index.get(store, object.key) == &object) live.push_back(offset);
        }

        if (!live.empty() && live.size() * 2 > tail) continue;
        const uint64_t owner = source->meta.owner_id.load();
        const uint64_t term = source->meta.term_id.load();
        const uint64_t destination_term = allocation_term ? allocation_term : term;
        Segment* destination = nullptr;
        
        if (!live.empty()) {
            for (Segment* candidate : store.segments) {
                if (candidate != source && candidate->meta.status.load() == static_cast<uint8_t>(SegmentStatus::ACTIVE) &&
                    candidate->meta.owner_id.load() == owner && candidate->meta.term_id.load() == destination_term &&
                    (!allocate || candidate->meta.committed.load()) &&
                    candidate->capacity - candidate->meta.tail_idx.load() >= live.size()) {
                    destination = candidate;
                    break;
                }
            }
            if (!destination) {
                // One failed ownership round is enough for this pass. Still
                // reclaim fully dead sources without repeating network waits.
                if (allocate && allocation_failed) continue;
                const int64_t allocated = allocate ? allocate(owner, destination_term)
                                                  : store.acquire_free_segment(owner, destination_term);
                if (allocated < 0) { allocation_failed = true; continue; }
                destination = store.segments[static_cast<size_t>(allocated)];
            }
            for (uint64_t offset : live) {
                const uint64_t moved = destination->append(source->entries[offset]);
                if (!index.relocate(store, source->seg_index, offset, destination->seg_index, moved)) {
                    throw std::runtime_error("GC could not relocate an indexed object");
                }
            }
            if (destination->is_full()) destination->seal();
        }
        if (store.release_sealed_segment(source->seg_index, owner, term, source->meta.seg_ver.load())) {
            ++released;
            // A subsequent allocation may release data_mutex for its quorum
            // round. Publish each completed layout change before that point.
            store.layout_version.fetch_add(1);
        }
    }
    if (released) {
        store.reclaimed_segments.fetch_add(released);
    }
    return released;
}
