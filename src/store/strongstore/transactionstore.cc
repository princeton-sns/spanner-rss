
#include "store/strongstore/transactionstore.h"

#include <algorithm>

namespace strongstore {

TransactionStore::TransactionStore(int this_shard, Consistency c, const TrueTime &tt)
    : this_shard_{this_shard}, consistency_{c}, tt_{tt} {}

TransactionStore::~TransactionStore() {}

void TransactionStore::PendingRWTransaction::StartGet(const TransportAddress &remote,
                                                      const std::string &key, bool for_update) {
    if (!client_addr_) {
        client_addr_.reset(remote.clone());
    }

    transaction_.addReadSet(key, Timestamp());

    if (for_update) {
        transaction_.addWriteSet(key, key);
    }
}

void TransactionStore::PendingRWTransaction::StartCoordinatorPrepare(const Timestamp &start_ts, int coordinator,
                                                                     const std::unordered_set<int> participants,
                                                                     const Transaction &transaction,
                                                                     const Timestamp &nonblock_ts) {
    ASSERT(participants.size() > 0);

    start_ts_ = start_ts;
    participants_ = participants;
    transaction_.add_read_write_sets(transaction);
    transaction_.set_start_time(transaction.start_time());
    prepare_ts_ = std::max(prepare_ts_, start_ts);
    Debug("Updating nonblock ts: %lu to %lu", nonblock_ts_.getTimestamp(), nonblock_ts.getTimestamp());
    nonblock_ts_ = std::max(nonblock_ts_, nonblock_ts);

    if (coordinator_ == -1) {
        coordinator_ = coordinator;
    } else {
        ASSERT(coordinator_ == coordinator);
    }

    std::size_t n = participants_.size();
    std::size_t ok = ok_participants_.size();
    if (ok == n - 1) {
        state_ = PREPARING;
    } else {
        state_ = WAIT_PARTICIPANTS;
    }
}

void TransactionStore::PendingRWTransaction::FinishCoordinatorPrepare(const Timestamp &prepare_ts) {
    prepare_ts_ = std::max(prepare_ts_, prepare_ts);
    commit_ts_ = prepare_ts_;
    state_ = COMMITTING;
}

void TransactionStore::PendingRWTransaction::StartParticipantPrepare(int coordinator,
                                                                     const Transaction &transaction,
                                                                     const Timestamp &nonblock_ts) {
    if (coordinator_ == -1) {
        coordinator_ = coordinator;
    } else {
        ASSERT(coordinator_ == coordinator);
    }

    transaction_.add_read_write_sets(transaction);
    Debug("Setting nonblock ts: %lu", nonblock_ts.getTimestamp());
    nonblock_ts_ = nonblock_ts;
    state_ = PREPARING;
}

void TransactionStore::PendingRWTransaction::SetParticipantPrepareTimestamp(const Timestamp &prepare_ts) {
    prepare_ts_ = prepare_ts;
}

void TransactionStore::PendingRWTransaction::FinishParticipantPrepare() {
    state_ = PREPARED;
}

void TransactionStore::PendingRWTransaction::ReceivePrepareOK(int coordinator, int participant,
                                                              const Timestamp &prepare_ts,
                                                              const Timestamp &nonblock_ts) {
    if (coordinator_ == -1) {
        coordinator_ = coordinator;
    } else {
        ASSERT(coordinator_ == coordinator);
    }

    prepare_ts_ = std::max(prepare_ts_, prepare_ts);
    Debug("Updating nonblock ts: %lu to %lu", nonblock_ts_.getTimestamp(), nonblock_ts.getTimestamp());
    nonblock_ts_ = std::max(nonblock_ts_, nonblock_ts);
    ok_participants_.insert(participant);

    std::size_t n = participants_.size();
    std::size_t ok = ok_participants_.size();
    if (n == 0 || ok != n - 1) {
        state_ = WAIT_PARTICIPANTS;
    } else {
        state_ = PREPARING;
    }
}

void TransactionStore::PendingROTransaction::StartRO(const std::unordered_set<std::string> &keys,
                                                     const Timestamp &min_ts,
                                                     const Timestamp &commit_ts,
                                                     uint64_t n_conflicts) {
    keys_.insert(keys.begin(), keys.end());
    min_ts_ = min_ts;
    commit_ts_ = commit_ts;
    n_conflicts_ = n_conflicts;
    if (n_conflicts_ == 0) {
        state_ = COMMITTING;
    } else {
        state_ = PREPARE_WAIT;
    }
}

void TransactionStore::StartGet(uint64_t transaction_id, const TransportAddress &remote, const std::string &key, bool for_update) {
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == READING);

    pt.StartGet(remote, key, for_update);
}

void TransactionStore::FinishGet(uint64_t transaction_id, const std::string &key) {
    (void)key;
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == READING);
}

void TransactionStore::AbortGet(uint64_t transaction_id, const std::string &key) {
    (void)key;
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == READING ||
           pt.state() == READ_WAIT);

    pending_rw_.erase(transaction_id);
    aborted_.insert(transaction_id);
}

void TransactionStore::PauseGet(uint64_t transaction_id, const std::string &key) {
    (void)key;
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == READING);

    pt.set_state(READ_WAIT);
}

TransactionState TransactionStore::ContinueGet(uint64_t transaction_id, const std::string &key) {
    (void)key;
    if (aborted_.count(transaction_id) > 0) {
        return ABORTED;
    }

    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == READ_WAIT);

    pt.set_state(READING);

    return pt.state();
}

TransactionState TransactionStore::StartCoordinatorPrepare(uint64_t transaction_id, const Timestamp &start_ts,
                                                           int coordinator, const std::unordered_set<int> participants,
                                                           const Transaction &transaction,
                                                           const Timestamp &nonblock_ts) {
    if (aborted_.count(transaction_id) > 0) {
        return ABORTED;
    }

    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == READING || pt.state() == WAIT_PARTICIPANTS);

    Debug("[%lu] Coordinator: StartTransaction %lu.%lu", transaction_id, start_ts.getTimestamp(), start_ts.getID());

    pt.StartCoordinatorPrepare(start_ts, coordinator, participants, transaction, nonblock_ts);

    return pt.state();
}

void TransactionStore::FinishCoordinatorPrepare(uint64_t transaction_id, const Timestamp &prepare_ts) {
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == PREPARING);
    pt.FinishCoordinatorPrepare(prepare_ts);
}

TransactionState TransactionStore::StartParticipantPrepare(uint64_t transaction_id, int coordinator,
                                                           const Transaction &transaction, const Timestamp &nonblock_ts) {
    if (aborted_.count(transaction_id) > 0) {
        return ABORTED;
    }

    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == READING);

    Debug("[%lu] Participant prepare", transaction_id);

    pt.StartParticipantPrepare(coordinator, transaction, nonblock_ts);

    return pt.state();
}

void TransactionStore::SetParticipantPrepareTimestamp(uint64_t transaction_id, const Timestamp &prepare_ts) {
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == PREPARING);

    pt.SetParticipantPrepareTimestamp(prepare_ts);
}

TransactionState TransactionStore::FinishParticipantPrepare(uint64_t transaction_id) {
    if (aborted_.count(transaction_id) > 0) {
        return ABORTED;
    }

    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == PREPARING);

    pt.FinishParticipantPrepare();

    return pt.state();
}

TransactionState TransactionStore::GetRWTransactionState(uint64_t transaction_id) {
    if (committed_.count(transaction_id) > 0) {
        return COMMITTED;
    }

    if (aborted_.count(transaction_id) > 0) {
        return ABORTED;
    }

    auto search = pending_rw_.find(transaction_id);
    if (search == pending_rw_.end()) {
        return NOT_FOUND;
    } else {
        return search->second.state();
    }
}

TransactionState TransactionStore::GetROTransactionState(uint64_t transaction_id) {
    if (committed_.count(transaction_id) > 0) {
        return COMMITTED;
    }

    ASSERT(aborted_.count(transaction_id) == 0);

    auto search = pending_ro_.find(transaction_id);
    if (search == pending_ro_.end()) {
        return NOT_FOUND;
    } else {
        return search->second.state();
    }
}

const Timestamp &TransactionStore::GetStartTimestamp(uint64_t transaction_id) {
    auto search = pending_rw_.find(transaction_id);
    ASSERT(search != pending_rw_.end());
    return search->second.start_ts();
}

const std::unordered_set<int> &TransactionStore::GetParticipants(uint64_t transaction_id) {
    auto search = pending_rw_.find(transaction_id);
    ASSERT(search != pending_rw_.end());
    return search->second.participants();
}

const Timestamp &TransactionStore::GetNonBlockTimestamp(uint64_t transaction_id) {
    auto search = pending_rw_.find(transaction_id);
    ASSERT(search != pending_rw_.end());
    return search->second.nonblock_ts();
}

const Transaction &TransactionStore::GetTransaction(uint64_t transaction_id) {
    auto search = pending_rw_.find(transaction_id);
    ASSERT(search != pending_rw_.end());
    return search->second.transaction();
}

int TransactionStore::GetCoordinator(uint64_t transaction_id) {
    auto search = pending_rw_.find(transaction_id);
    ASSERT(search != pending_rw_.end());
    return search->second.coordinator();
}

std::shared_ptr<TransportAddress> TransactionStore::GetClientAddr(uint64_t transaction_id) {
    auto search = pending_rw_.find(transaction_id);
    ASSERT(search != pending_rw_.end());
    return search->second.client_addr();
}

const Timestamp &TransactionStore::GetPrepareTimestamp(uint64_t transaction_id) {
    auto search = pending_rw_.find(transaction_id);
    ASSERT(search != pending_rw_.end());
    return search->second.prepare_ts();
}

const Timestamp &TransactionStore::GetRWCommitTimestamp(uint64_t transaction_id) {
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == COMMITTING);
    return pt.commit_ts();
}

const Timestamp &TransactionStore::GetROCommitTimestamp(uint64_t transaction_id) {
    PendingROTransaction &pt = pending_ro_[transaction_id];
    ASSERT(pt.state() == COMMITTING);
    return pt.commit_ts();
}

const std::unordered_set<std::string> &TransactionStore::GetROKeys(uint64_t transaction_id) {
    PendingROTransaction &pt = pending_ro_[transaction_id];
    ASSERT(pt.state() == COMMITTING);
    return pt.keys();
}

void TransactionStore::AbortPrepare(uint64_t transaction_id) {
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == PREPARING ||
           pt.state() == PREPARE_WAIT ||
           pt.state() == WAIT_PARTICIPANTS);

    pending_rw_.erase(transaction_id);
    aborted_.insert(transaction_id);
}

void TransactionStore::PausePrepare(uint64_t transaction_id) {
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == PREPARING);
    ASSERT(pt.wait_start() == 0);

    pt.set_state(PREPARE_WAIT);
    pt.set_wait_start(tt_.Now().mid());
}

TransactionState TransactionStore::ContinuePrepare(uint64_t transaction_id) {
    if (aborted_.count(transaction_id) > 0) {
        return ABORTED;
    }

    PendingRWTransaction &pt = pending_rw_[transaction_id];
    if (pt.state() == PREPARE_WAIT) {
        pt.set_state(PREPARING);

        uint64_t diff = tt_.Now().mid() - pt.wait_start();
        pt.advance_nonblock_ts(diff);
        pt.set_wait_start(0);
        Debug("[%lu] Advancing nonblock ts by %lu micros: %lu", transaction_id, diff, pt.nonblock_ts().getTimestamp());
    }

    return pt.state();
}

TransactionState TransactionStore::CoordinatorReceivePrepareOK(uint64_t transaction_id, int participant,
                                                               const Timestamp &prepare_ts,
                                                               const Timestamp &nonblock_ts) {
    if (aborted_.count(transaction_id) > 0) {
        return ABORTED;
    }

    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == READING || pt.state() == WAIT_PARTICIPANTS);
    pt.ReceivePrepareOK(this_shard_, participant, prepare_ts, nonblock_ts);

    return pt.state();
}

TransactionState TransactionStore::ParticipantReceivePrepareOK(uint64_t transaction_id) {
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == PREPARED);

    pt.set_state(COMMITTING);

    return pt.state();
}

void TransactionStore::NotifyROs(std::unordered_set<uint64_t> &ros) {
    for (auto it = ros.begin(); it != ros.end();) {
        ASSERT(pending_ro_.find(*it) != pending_ro_.end());
        PendingROTransaction &pt = pending_ro_[*it];
        pt.decr_conflicts();

        if (pt.n_conflicts() > 0) {
            it = ros.erase(it);
        } else {
            it++;
        }
    }
}

TransactionFinishResult TransactionStore::Commit(uint64_t transaction_id) {
    // Debug("[%lu] COMMIT", transaction_id);

    TransactionFinishResult r;

    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == COMMITTING);

    r.notify_ros = std::move(pt.waiting_ros());
    NotifyROs(r.notify_ros);

    r.notify_slow_path_ros = std::move(pt.slow_path_ros());

    pending_rw_.erase(transaction_id);
    committed_.insert(transaction_id);

    return r;
}

TransactionFinishResult TransactionStore::Abort(uint64_t transaction_id) {
    // Debug("[%lu] ABORT", transaction_id);

    TransactionFinishResult r;

    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() != COMMITTING &&
           pt.state() != COMMITTED &&
           pt.state() != ABORTED);

    r.notify_ros = std::move(pt.waiting_ros());
    NotifyROs(r.notify_ros);

    r.notify_slow_path_ros = std::move(pt.slow_path_ros());

    pending_rw_.erase(transaction_id);
    aborted_.insert(transaction_id);

    return r;
}

TransactionState TransactionStore::StartRO(uint64_t transaction_id,
                                           const std::unordered_set<std::string> &keys,
                                           const Timestamp &min_ts,
                                           const Timestamp &commit_ts) {
    PendingROTransaction &ro = pending_ro_[transaction_id];
    ASSERT(ro.state() == PREPARING);

    uint64_t n_skipped = 0;
    uint64_t n_conflicts = 0;

    ASSERT(min_ts < commit_ts);

    for (auto &p : pending_rw_) {
        PendingRWTransaction &rw = p.second;

        if (rw.state() != PREPARING &&
            rw.state() != PREPARED &&
            rw.state() != COMMITTING) {
            continue;
        }

        if (commit_ts < rw.prepare_ts()) {
            Debug("[%lu] Not waiting for prepared transaction (prepare): %lu < %lu",
                  transaction_id, commit_ts.getTimestamp(),
                  rw.prepare_ts().getTimestamp());
            continue;
        }

        const Transaction &transaction = rw.transaction();
        for (auto &w : transaction.getWriteSet()) {
            if (keys.count(w.first) != 0) {
                Debug("%lu conflicts with %lu", transaction_id, p.first);
                if (consistency_ == Consistency::RSS &&
                    min_ts < rw.prepare_ts() && commit_ts < rw.nonblock_ts()) {
                    Debug("[%lu] Not waiting for prepared transaction (nonblock): %lu < %lu",
                          transaction_id, commit_ts.getTimestamp(),
                          rw.nonblock_ts().getTimestamp());

                    ro.add_skipped_rw(p.first);
                    rw.add_slow_path_ro(transaction_id);
                    n_skipped += 1;
                } else {
                    rw.add_waiting_ro(transaction_id);
                    n_conflicts += 1;
                }

                break;
            }
        }
    }

    ro.StartRO(keys, min_ts, commit_ts, n_conflicts);

    stats_.IncrementList("n_conflicting_prepared", n_conflicts);
    return ro.state();
}

void TransactionStore::ContinueRO(uint64_t transaction_id) {
    PendingROTransaction &ro = pending_ro_[transaction_id];
    ASSERT(ro.state() == PREPARE_WAIT);

    ro.set_state(COMMITTING);
}

uint64_t TransactionStore::GetRONumberSkipped(uint64_t transaction_id) {
    PendingROTransaction &ro = pending_ro_[transaction_id];
    ASSERT(ro.state() == PREPARE_WAIT || ro.state() == COMMITTING);

    return ro.skipped_rws().size();
}

std::vector<PreparedTransaction> TransactionStore::GetROSkippedRWTransactions(uint64_t transaction_id) {
    ASSERT(consistency_ == RSS);

    PendingROTransaction &ro = pending_ro_[transaction_id];
    ASSERT(ro.state() == COMMITTING);

    std::vector<PreparedTransaction> skipped;

    for (uint64_t rw : ro.skipped_rws()) {
        TransactionState s = GetRWTransactionState(rw);
        if (s == PREPARING || s == PREPARED || s == COMMITTING) {
            auto search = pending_rw_.find(rw);
            ASSERT(search != pending_rw_.end());
            PendingRWTransaction &prw = search->second;

            ASSERT(ro.min_ts() < prw.prepare_ts() && ro.commit_ts() < prw.nonblock_ts());

            const std::unordered_set<std::string> &keys = ro.keys();
            bool first = true;
            for (auto &w : prw.transaction().getWriteSet()) {
                if (keys.count(w.first) > 0) {
                    if (first) {
                        skipped.emplace_back(rw, prw.prepare_ts());
                        first = false;
                    }

                    skipped.back().add_write_set(w);
                }
            }
        }
    }

    return skipped;
}

void TransactionStore::StartROSlowPath(uint64_t transaction_id) {
    ASSERT(consistency_ == RSS);

    PendingROTransaction &ro = pending_ro_[transaction_id];
    ASSERT(ro.state() == COMMITTING);

    ro.set_state(SLOW_PATH);
}

void TransactionStore::FinishROSlowPath(uint64_t transaction_id) {
    ASSERT(consistency_ == RSS);

    PendingROTransaction &ro = pending_ro_[transaction_id];
    ASSERT(ro.state() == SLOW_PATH);

    ro.set_state(COMMITTED);
    pending_ro_.erase(transaction_id);
    committed_.insert(transaction_id);
}

void TransactionStore::CommitRO(uint64_t transaction_id) {
    PendingROTransaction &ro = pending_ro_[transaction_id];
    ASSERT(ro.state() == COMMITTING);

    ro.set_state(COMMITTED);
    pending_ro_.erase(transaction_id);
    committed_.insert(transaction_id);
}

}  // namespace strongstore
