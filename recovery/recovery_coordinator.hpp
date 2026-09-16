#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>
#include <unistd.h>

#include "../engine/put_path.hpp"
#include "../index/hash_table.hpp"
#include "../logging/log.hpp"
#include "../network/endpoint.hpp"
#include "../network/message.hpp"
#include "../network/socket_utils.hpp"
#include "../replica/replica_state.hpp"
#include "../storage/segment_store.hpp"

// Recovery selects a source independently for each worker because different
// worker streams may have reached different replica majorities.
class RecoveryCoordinator {
public:
    RecoveryCoordinator(ReplicaState& replica_state, SegmentStore& segment_store,
                        HashTable& index,
                        const std::vector<PeerEndpoint>& endpoints,
                        size_t worker_count)
        : replica_state_(replica_state),
          store_(segment_store),
          index_(index),
          endpoints_(endpoints),
          worker_count_(worker_count),
          // Unreachable endpoints stay in the quorum denominator. The caller
          // must supply the full membership; this does not validate CLI changes.
          required_survivors_((endpoints.size() + 1) / 2 + 1) {}

    uint64_t recover() {
        return recover_impl(0);
    }

    // The caller must have won this term, block client access, quiesce local
    // appends, and exclude concurrent follower installation for this call.
    // It must maintain election heartbeats and recheck leadership before
    // enabling client access. This method does not elect or publish a leader.
    uint64_t recover_elected(uint64_t election_term) {
        if (election_term == 0) {
            throw std::invalid_argument("elected recovery requires a term");
        }
        return recover_impl(election_term);
    }

private:
    uint64_t recover_impl(uint64_t election_term) {
        elected_term_ = election_term;
        recovery_term_ = 0; // Initial summaries precede recovery authorization.
        peers_.clear();
        local_records_.clear();
        local_progress_.clear();
        check_elected_epoch();
        if (elected_term_ != 0) snapshot_local_history();
        DISKEYV_INFO("RECOVERY",
                     "replica=" << replica_state_.replica_id
                                << " phase=start followers="
                                << endpoints_.size() << " workers="
                                << worker_count_);
        if (endpoints_.empty() && elected_term_ == 0) {
            throw std::runtime_error(
                "recovery requires at least one surviving follower");
        }

        // Startup recovery chooses a new term. An elected survivor must use
        // exactly the term it won; incrementing it would invalidate its votes.
        connect_and_collect_summaries();
        recovery_term_ = elected_term_ != 0 ? elected_term_ : choose_new_term();
        check_elected_epoch();
        DISKEYV_INFO("RECOVERY",
                     "replica=" << replica_state_.replica_id
                                << " phase=term-selected term="
                                << recovery_term_);
        advance_follower_terms(recovery_term_);
        DISKEYV_INFO("RECOVERY",
                     "replica=" << replica_state_.replica_id
                                << " phase=fenced followers=" << peers_.size());
        refresh_stable_summaries();
        // Different workers can have different freshest replicas, so source
        // selection is deliberately independent for every worker stream.
        const std::vector<SelectedPrefix> selected = select_worker_sources();

        std::vector<ReplicationEntry> canonical_records;
        for (uint64_t worker_id = 0; worker_id < worker_count_; ++worker_id) {
            const SelectedPrefix& prefix = selected[worker_id];
            if (prefix.sequence == 0) continue;
            std::vector<ReplicationEntry> records;
            if (prefix.peer_index == kLocalSource) {
                for (const auto& entry : local_records_) {
                    if (entry.worker_id == worker_id) records.push_back(entry);
                }
            } else {
                records = fetch_worker_history(*peers_[prefix.peer_index],
                                              worker_id, prefix.term,
                                              prefix.sequence);
            }
            DISKEYV_INFO("RECOVERY",
                         "replica=" << replica_state_.replica_id
                                    << " phase=fetched worker=" << worker_id
                                    << " source="
                                    << source_name(prefix)
                                    << " prefix=<" << prefix.term << ','
                                    << prefix.sequence << "> records="
                                    << records.size());
            canonical_records.insert(
                canonical_records.end(),
                std::make_move_iterator(records.begin()),
                std::make_move_iterator(records.end())
            );
        }

        // Preserve earlier local terms too. A selected remote source can have
        // a newer stream while this survivor still holds older useful records.
        // Exact duplicates are collapsed; conflicting offsets fail closed.
        canonical_records.insert(canonical_records.end(),
                                 local_records_.begin(), local_records_.end());
        check_elected_epoch();
        validate_and_import(canonical_records, selected);
        DISKEYV_INFO("RECOVERY",
                     "replica=" << replica_state_.replica_id
                                << " phase=imported records="
                                << canonical_records.size());
        repair_followers(canonical_records);
        refresh_stable_summaries();
        verify_repaired_followers(selected);
        check_elected_epoch();
        rebuild_index(canonical_records);
        seal_recovered_segments();
        if (elected_term_ == 0) {
            replica_state_.current_term.store(recovery_term_, std::memory_order_release);
        }
        check_elected_epoch();
        DISKEYV_INFO("RECOVERY",
                     "replica=" << replica_state_.replica_id
                                << " phase=complete term=" << recovery_term_
                                << " records=" << canonical_records.size());
        return recovery_term_;
    }

    struct PeerSession {
        explicit PeerSession(PeerEndpoint peer_endpoint)
            : endpoint(std::move(peer_endpoint)) {}

        ~PeerSession() {
            if (socket >= 0) {
                ::shutdown(socket, SHUT_RDWR);
                close(socket);
            }
        }

        PeerSession(const PeerSession&) = delete;
        PeerSession& operator=(const PeerSession&) = delete;

        PeerEndpoint endpoint;
        int socket{-1}; // Reused across all recovery phases for this follower.
        uint64_t current_term{0}; // Term reported by its latest summary.
        // worker_id -> latest contiguous progress advertised by the follower.
        std::map<uint64_t, WorkerProgressWire> progress;
    };

    static constexpr size_t kLocalSource = std::numeric_limits<size_t>::max();

    struct SelectedPrefix {
        uint64_t term{0};       // Freshest term selected for one worker.
        uint64_t sequence{0};   // Contiguous upper bound copied from source.
        size_t peer_index{kLocalSource}; // Local snapshot or session in peers_.
    };

    ReplicaState& replica_state_;
    SegmentStore& store_;
    HashTable& index_;
    const std::vector<PeerEndpoint>& endpoints_;
    size_t worker_count_;
    // Full majority for an empty replacement; an elected survivor contributes
    // its own retained history, reducing the required remote count by one.
    size_t required_survivors_;
    uint64_t recovery_term_{0}; // Fencing term used for this consolidation.
    uint64_t elected_term_{0};  // Zero selects explicit startup recovery.
    std::vector<std::unique_ptr<PeerSession>> peers_;
    std::vector<ReplicationEntry> local_records_;
    std::map<uint64_t, WorkerProgressWire> local_progress_;

    void check_elected_epoch() const {
        if (elected_term_ == 0) return;
        std::lock_guard<std::mutex> lock(replica_state_.election_mutex);
        if (replica_state_.current_term.load(std::memory_order_acquire) !=
                elected_term_ ||
            replica_state_.role.load(std::memory_order_acquire) == Role::FOLLOWER ||
            replica_state_.voted_term != elected_term_ ||
            replica_state_.voted_for != replica_state_.replica_id) {
            throw std::runtime_error("leadership changed during recovery");
        }
    }

    std::string source_name(const SelectedPrefix& prefix) const {
        return prefix.peer_index == kLocalSource
                   ? "local"
                   : peers_[prefix.peer_index]->endpoint.identity();
    }

    // Scan the survivor while its caller holds the local recovery barrier.
    // Segment ownership supplies the worker ID absent from ObjectEntry itself.
    void snapshot_local_history() {
        std::map<uint64_t, std::map<uint64_t, std::set<uint64_t>>> sequences;
        for (const Segment* segment : store_.segments) {
            if (segment->meta.status.load(std::memory_order_acquire) ==
                static_cast<uint8_t>(SegmentStatus::FREE)) continue;
            const uint64_t worker = segment->meta.owner_id.load(std::memory_order_acquire);
            const uint64_t tail = segment->meta.tail_idx.load(std::memory_order_acquire);
            if (worker >= worker_count_ || tail > segment->capacity) {
                throw std::runtime_error("invalid local recovery segment");
            }
            for (uint64_t offset = 0; offset < tail; ++offset) {
                const ObjectEntry& object = segment->entries[offset];
                if (object.term_id == 0 || object.term_id >= elected_term_ ||
                    object.seq_num == 0 || object.key[0] == '\0' ||
                    !sequences[worker][object.term_id].insert(object.seq_num).second) {
                    throw std::runtime_error("invalid local recovery record");
                }
                ReplicationEntry entry;
                entry.worker_id = worker;
                entry.term = object.term_id;
                entry.sequence = object.seq_num;
                entry.incarnation = object.incarnation;
                entry.segment_index = segment->seg_index;
                entry.object_index = offset;
                entry.key = std::string(object.key);
                entry.value = object.value;
                local_records_.push_back(std::move(entry));
            }
        }
        for (const auto& worker : sequences) {
            for (const auto& term : worker.second) {
                uint64_t expected = 1;
                for (uint64_t sequence : term.second) {
                    if (sequence != expected++) {
                        throw std::runtime_error("local recovery stream has a gap");
                    }
                }
                local_progress_[worker.first] =
                    WorkerProgressWire{worker.first, term.first, *term.second.rbegin()};
            }
        }
    }

    static bool later(uint64_t left_term, uint64_t left_sequence,
                      uint64_t right_term, uint64_t right_sequence) {
        return std::tie(left_term, left_sequence) >
               std::tie(right_term, right_sequence);
    }

    static bool exchange(PeerSession& peer, const NetMessage& request,
                         NetMessage& reply) {
        return send_message(peer.socket, request) &&
               recv_message(peer.socket, reply);
    }

    void connect_and_collect_summaries() {
        peers_.reserve(endpoints_.size());
        for (const PeerEndpoint& endpoint : endpoints_) {
            auto peer = std::make_unique<PeerSession>(endpoint);
            peer->socket = connect_to(endpoint.host, endpoint.port);
            if (peer->socket < 0) {
                DISKEYV_WARN("RECOVERY",
                             "replica=" << replica_state_.replica_id
                                        << " follower-unreachable="
                                        << endpoint.identity()
                                        << " action=excluded-from-active-set");
                continue;
            }

            try {
                collect_summary(*peer);
            } catch (const std::exception& error) {
                DISKEYV_WARN("RECOVERY",
                             "replica=" << replica_state_.replica_id
                                        << " follower-unavailable="
                                        << endpoint.identity()
                                        << " reason=" << error.what()
                                        << " action=excluded-from-active-set");
                continue;
            }
            check_elected_epoch();
            if (elected_term_ != 0 && peer->current_term > elected_term_) {
                throw std::runtime_error("recovery peer has a newer election term");
            }
            DISKEYV_INFO("RECOVERY",
                         "replica=" << replica_state_.replica_id
                                    << " connected=" << endpoint.identity()
                                    << " follower-term=" << peer->current_term
                                    << " worker-summaries="
                                    << peer->progress.size());
            peers_.push_back(std::move(peer));
        }
        const size_t required_remote = required_survivors_ - (elected_term_ != 0 ? 1U : 0U);
        if (peers_.size() < required_remote) {
            throw std::runtime_error(
                "recovery reached " + std::to_string(peers_.size()) +
                " surviving followers but requires " +
                std::to_string(required_remote) +
                " from the configured membership");
        }
        DISKEYV_INFO("RECOVERY",
                     "replica=" << replica_state_.replica_id
                                << " active-followers=" << peers_.size()
                                << " required-survivors="
                                << required_remote
                                << " configured-members="
                                << endpoints_.size() + 1);
    }

    void collect_summary(PeerSession& peer) {
        NetMessage request;
        request.type = MsgType::RECOVERY_SUMMARY_REQUEST;
        request.term = recovery_term_;
        request.segment_index = replica_state_.replica_id;
        NetMessage reply;
        if (!exchange(peer, request, reply) ||
            reply.type != MsgType::RECOVERY_SUMMARY_REPLY ||
            reply.status != OperationStatus::OK || reply.term == 0 ||
            (recovery_term_ != 0 && reply.term != recovery_term_)) {
            throw std::runtime_error(
                "invalid recovery summary from " + peer.endpoint.identity());
        }
        peer.current_term = reply.term;
        peer.progress.clear();
        for (const WorkerProgressWire& progress : reply.worker_progress) {
            if (progress.worker_id >= worker_count_ ||
                (elected_term_ != 0 && progress.term >= elected_term_) ||
                !peer.progress.emplace(progress.worker_id, progress).second) {
                throw std::runtime_error(
                    "invalid worker metadata from " + peer.endpoint.identity());
            }
        }
    }

    void refresh_stable_summaries() {
        for (const auto& peer : peers_) {
            check_elected_epoch();
            collect_summary(*peer);
        }
    }

    // Choose the lexicographically greatest <term, sequence> for each worker.
    // This is the static-leader equivalent of LoLKV data consolidation.
    std::vector<SelectedPrefix> select_worker_sources() const {
        std::vector<SelectedPrefix> selected(worker_count_);
        for (const auto& item : local_progress_) {
            const WorkerProgressWire& progress = item.second;
            selected[progress.worker_id] =
                SelectedPrefix{progress.term, progress.sequence, kLocalSource};
        }
        for (size_t peer_index = 0; peer_index < peers_.size(); ++peer_index) {
            for (const auto& item : peers_[peer_index]->progress) {
                const WorkerProgressWire& candidate = item.second;
                SelectedPrefix& current = selected[candidate.worker_id];
                if (later(candidate.term, candidate.sequence, current.term,
                          current.sequence)) {
                    current.term = candidate.term;
                    current.sequence = candidate.sequence;
                    current.peer_index = peer_index;
                }
            }
        }
        for (size_t worker_id = 0; worker_id < selected.size(); ++worker_id) {
            const SelectedPrefix& prefix = selected[worker_id];
            if (prefix.sequence == 0) {
                DISKEYV_DEBUG("RECOVERY",
                              "worker=" << worker_id << " prefix=empty");
                continue;
            }
            DISKEYV_INFO("RECOVERY",
                         "worker=" << worker_id << " selected="
                                   << source_name(prefix)
                                   << " prefix=<" << prefix.term << ','
                                   << prefix.sequence << '>');
        }
        return selected;
    }

    void verify_repaired_followers(
        const std::vector<SelectedPrefix>& selected) const {
        for (const auto& peer : peers_) {
            for (size_t worker_id = 0; worker_id < selected.size();
                 ++worker_id) {
                const SelectedPrefix& expected = selected[worker_id];
                if (expected.sequence == 0) continue;
                const auto actual = peer->progress.find(worker_id);
                if (actual == peer->progress.end() ||
                    actual->second.term != expected.term ||
                    actual->second.sequence != expected.sequence) {
                    throw std::runtime_error(
                        "follower repair did not converge: " +
                        peer->endpoint.identity());
                }
            }
        }
    }


    std::vector<ReplicationEntry> fetch_worker_history(
        PeerSession& source, uint64_t worker_id, uint64_t upper_term,
        uint64_t upper_sequence) const {
        std::vector<ReplicationEntry> records;
        uint64_t cursor = 0;
        while (true) {
            check_elected_epoch();
            NetMessage request;
            request.type = MsgType::RECOVERY_FETCH_REQUEST;
            request.worker_id = worker_id;
            request.term = recovery_term_;
            request.segment_index = replica_state_.replica_id;
            request.incarnation = upper_term;
            request.object_index = upper_sequence;
            request.seq = cursor;

            NetMessage reply;
            if (!exchange(source, request, reply) ||
                reply.type != MsgType::RECOVERY_FETCH_REPLY ||
                reply.status != OperationStatus::OK ||
                reply.term != recovery_term_ ||
                reply.worker_id != worker_id || reply.object_index > 1 ||
                (!reply.object_index && reply.seq <= cursor)) {
                throw std::runtime_error(
                    "invalid recovery data from " + source.endpoint.identity());
            }
            for (ReplicationEntry& entry : reply.entries) {
                if (entry.worker_id != worker_id ||
                    later(entry.term, entry.sequence, upper_term,
                          upper_sequence)) {
                    throw std::runtime_error(
                        "recovery source exceeded its advertised prefix");
                }
                records.push_back(std::move(entry));
            }
            cursor = reply.seq;
            if (reply.object_index == 1) break;
        }
        return records;
    }

    static bool same_record(const ReplicationEntry& left,
                            const ReplicationEntry& right) {
        return left.worker_id == right.worker_id && left.term == right.term &&
               left.sequence == right.sequence && left.incarnation == right.incarnation &&
               left.segment_index == right.segment_index &&
               left.object_index == right.object_index &&
               left.key == right.key && left.value == right.value;
    }

    // Validate the full union before importing. A survivor already has part of
    // the union, so matching local offsets are accepted without appending twice.
    void validate_and_import(
        std::vector<ReplicationEntry>& records,
        const std::vector<SelectedPrefix>& selected) {
        std::sort(records.begin(), records.end(),
                  [](const ReplicationEntry& left,
                     const ReplicationEntry& right) {
                      return std::tie(left.segment_index, left.object_index) <
                             std::tie(right.segment_index, right.object_index);
                  });

        records.erase(std::unique(records.begin(), records.end(), same_record),
                      records.end());

        std::vector<std::map<uint64_t, std::set<uint64_t>>> sequences(
            worker_count_);
        std::unordered_set<std::string> offsets;
        for (const ReplicationEntry& entry : records) {
            if (entry.worker_id >= worker_count_ || entry.term == 0 ||
                entry.sequence == 0 ||
                entry.segment_index >= store_.segments.size()) {
                throw std::runtime_error("invalid object in recovery stream");
            }
            const std::string offset = std::to_string(entry.segment_index) +
                                       ":" +
                                       std::to_string(entry.object_index);
            if (!offsets.insert(offset).second ||
                !sequences[entry.worker_id][entry.term]
                     .insert(entry.sequence)
                     .second) {
                throw std::runtime_error("duplicate object in recovery stream");
            }

        }

        for (size_t worker_id = 0; worker_id < worker_count_; ++worker_id) {
            for (const auto& term_sequences : sequences[worker_id]) {
                uint64_t expected = 1;
                for (uint64_t sequence : term_sequences.second) {
                    if (sequence != expected++) {
                        throw std::runtime_error(
                            "recovery stream contains a worker sequence gap");
                    }
                }
            }
            const SelectedPrefix& expected_latest = selected[worker_id];
            if (expected_latest.sequence == 0) {
                if (!sequences[worker_id].empty()) {
                    throw std::runtime_error(
                        "unexpected worker records without a summary");
                }
                continue;
            }
            const auto term = sequences[worker_id].find(expected_latest.term);
            if (term == sequences[worker_id].end() || term->second.empty() ||
                *term->second.rbegin() != expected_latest.sequence) {
                throw std::runtime_error(
                    "recovery source did not provide its advertised prefix");
            }
        }
        for (const ReplicationEntry& entry : records) {
            check_elected_epoch();
            Segment& segment = *store_.segments[entry.segment_index];
            if (entry.object_index < segment.meta.tail_idx.load(std::memory_order_acquire)) {
                if (entry.object_index >= segment.capacity) {
                    throw std::runtime_error("recovery offset exceeds segment capacity");
                }
                const ObjectEntry& existing = segment.entries[entry.object_index];
                if (segment.meta.owner_id.load(std::memory_order_acquire) != entry.worker_id ||
                    segment.meta.term_id.load(std::memory_order_acquire) != entry.term ||
                    existing.term_id != entry.term || existing.seq_num != entry.sequence ||
                    existing.incarnation != entry.incarnation ||
                    std::string(existing.key) != entry.key || existing.value != entry.value) {
                    throw std::runtime_error("conflicting local recovery offset");
                }
                continue;
            }
            ApplyRecord applied{};
            if (!PutPath::put_replicated_at(
                    store_, entry.worker_id,
                    static_cast<size_t>(entry.segment_index),
                    entry.object_index, entry.key, entry.value, entry.term,
                    entry.sequence, entry.incarnation, applied, true)) {
                throw std::runtime_error("recovery could not reproduce a segment offset");
            }
        }
    }

    // Send the same canonical physical record set to every follower in bounded
    // recovery batches so all replicas converge before the index is exposed.
    void repair_followers(const std::vector<ReplicationEntry>& records) {
        for (const auto& peer : peers_) {
            size_t batches = 0;
            for (size_t first = 0; first < records.size();) {
                check_elected_epoch();
                size_t last = first;
                size_t batch_bytes = 0;
                while (last < records.size() &&
                       last - first < kMaxWireRecoveryBatchEntries) {
                    const ReplicationEntry& entry = records[last];
                    const size_t entry_bytes = entry.key.size() +
                                               entry.value.size() +
                                               8U * sizeof(uint64_t);
                    if (last != first &&
                        batch_bytes + entry_bytes > kMaxWireRecoveryBatchBytes) {
                        break;
                    }
                    batch_bytes += entry_bytes;
                    ++last;
                }
                NetMessage request;
                request.type = MsgType::RECOVERY_INSTALL_BATCH;
                request.term = recovery_term_;
                request.segment_index = replica_state_.replica_id;
                request.entries.assign(records.begin() +
                                           static_cast<std::ptrdiff_t>(first),
                                       records.begin() +
                                           static_cast<std::ptrdiff_t>(last));
                NetMessage reply;
                if (!exchange(*peer, request, reply) ||
                    reply.type != MsgType::ACK ||
                    reply.status != OperationStatus::OK ||
                    reply.term != recovery_term_) {
                    throw std::runtime_error(
                        "failed to repair follower " +
                        peer->endpoint.identity());
                }
                ++batches;
                first = last;
            }
            DISKEYV_INFO("RECOVERY",
                         "phase=repaired follower="
                             << peer->endpoint.identity() << " records="
                             << records.size() << " batches=" << batches);
        }
    }

    // Replay older versions before newer ones. HashTable ordering would reject
    // stale replacements either way, but deterministic order aids diagnosis.
    void rebuild_index(const std::vector<ReplicationEntry>& records) {
        std::vector<const ReplicationEntry*> ordered;
        ordered.reserve(records.size());
        for (const ReplicationEntry& entry : records) ordered.push_back(&entry);
        std::sort(ordered.begin(), ordered.end(),
                  [](const ReplicationEntry* left,
                     const ReplicationEntry* right) {
                      return std::tie(left->term, left->sequence,
                                      left->worker_id) <
                             std::tie(right->term, right->sequence,
                                      right->worker_id);
                  });
        for (const ReplicationEntry* entry : ordered) {
            const IndexApplyResult result = index_.apply(
                store_, static_cast<size_t>(entry->segment_index),
                entry->object_index
            );
            if (!index_apply_succeeded(result)) {
                throw std::runtime_error("failed to rebuild recovery index");
            }
        }
        DISKEYV_INFO("RECOVERY",
                     "phase=index-rebuilt records=" << records.size());
    }

    void seal_recovered_segments() {
        for (Segment* segment : store_.segments) {
            if (segment->meta.status.load(std::memory_order_acquire) ==
                static_cast<uint8_t>(SegmentStatus::ACTIVE)) {
                segment->seal();
            }
        }
    }

    // Recovery owns a strictly newer epoch than the replacement leader and all
    // surviving followers, which is the basis of subsequent request fencing.
    uint64_t choose_new_term() const {
        uint64_t largest_term =
            replica_state_.current_term.load(std::memory_order_acquire);
        for (const auto& peer : peers_) {
            largest_term = std::max(largest_term, peer->current_term);
        }
        if (largest_term == std::numeric_limits<uint64_t>::max()) {
            throw std::runtime_error("term number exhausted");
        }
        return largest_term + 1;
    }

    void advance_follower_terms(uint64_t new_term) {
        for (const auto& peer : peers_) {
            check_elected_epoch();
            NetMessage request;
            request.type = MsgType::TERM_ADVANCE;
            request.term = new_term;
            request.incarnation = replica_state_.replica_id;
            NetMessage reply;
            if (!exchange(*peer, request, reply) ||
                reply.type != MsgType::TERM_ADVANCE_REPLY ||
                reply.status != OperationStatus::OK ||
                reply.term != new_term) {
                throw std::runtime_error(
                    "failed to advance follower term " +
                    peer->endpoint.identity());
            }
            DISKEYV_DEBUG("RECOVERY",
                          "term-advanced follower="
                              << peer->endpoint.identity() << " term="
                              << new_term);
        }
    }
};
