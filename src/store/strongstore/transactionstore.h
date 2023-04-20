/***********************************************************************
 *
 * store/strongstore/transactionstore.h:
 *
 * Copyright 2022 Jeffrey Helt, Matthew Burke, Amit Levy, Wyatt Lloyd
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/
#ifndef _STRONG_TRANSACTION_STORE_H_
#define _STRONG_TRANSACTION_STORE_H_

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "store/common/stats.h"
#include "store/common/transaction.h"
#include "store/common/truetime.h"
#include "store/strongstore/common.h"
#include "store/strongstore/preparedtransaction.h"

namespace strongstore
{

    enum TransactionState
    {
        NOT_FOUND,
        READING,
        READ_WAIT,
        PREPARING,
        WAIT_PARTICIPANTS,
        PREPARE_WAIT,
        PREPARED,
        COMMITTING,
        COMMITTED,
        ABORTED,
        SLOW_PATH
    };

    struct TransactionFinishResult
    {
        std::unordered_set<uint64_t> notify_ros;
        std::unordered_set<uint64_t> notify_slow_path_ros;
    };

    class TransactionStore
    {
    public:
        TransactionStore(int this_shard, Consistency consistency, const TrueTime &tt);
        ~TransactionStore();

        TransactionState GetRWTransactionState(uint64_t transaction_id);
        TransactionState GetROTransactionState(uint64_t transaction_id);

        const Transaction &GetTransaction(uint64_t transaction_id);
        const Timestamp &GetPrepareTimestamp(uint64_t transaction_id);
        const Timestamp &GetRWCommitTimestamp(uint64_t transaction_id);
        const Timestamp &GetStartTimestamp(uint64_t transaction_id);
        const std::unordered_set<int> &GetParticipants(uint64_t transaction_id);
        const Timestamp &GetNonBlockTimestamp(uint64_t transaction_id);
        int GetCoordinator(uint64_t transaction_id);
        std::shared_ptr<TransportAddress> GetClientAddr(uint64_t transaction_id);

        const Timestamp &GetROCommitTimestamp(uint64_t transaction_id);
        const std::unordered_set<std::string> &GetROKeys(uint64_t transaction_id);

        TransactionState StartRO(uint64_t transaction_id,
                                 const std::unordered_set<std::string> &keys,
                                 const Timestamp &min_ts,
                                 const Timestamp &commit_ts);
        void ContinueRO(uint64_t transaction_id);
        void CommitRO(uint64_t transaction_id);

        void StartROSlowPath(uint64_t transaction_id);
        void FinishROSlowPath(uint64_t transaction_id);
        std::vector<PreparedTransaction> GetROSkippedRWTransactions(uint64_t transaction_id);
        uint64_t GetRONumberSkipped(uint64_t transaction_id);

        void StartGet(uint64_t transaction_id, const TransportAddress &remote, const std::string &key, bool for_update);
        void FinishGet(uint64_t transaction_id, const std::string &key);
        void AbortGet(uint64_t transaction_id, const std::string &key);
        void PauseGet(uint64_t transaction_id, const std::string &key);
        TransactionState ContinueGet(uint64_t transaction_id, const std::string &key);

        TransactionState StartCoordinatorPrepare(uint64_t transaction_id, const Timestamp &start_ts,
                                                 int coordinator, const std::unordered_set<int> participants,
                                                 const Transaction &transaction,
                                                 const Timestamp &nonblock_ts);
        void FinishCoordinatorPrepare(uint64_t transaction_id, const Timestamp &prepare_ts);

        TransactionState StartParticipantPrepare(uint64_t transaction_id, int coordinator,
                                                 const Transaction &transaction, const Timestamp &nonblock_ts);
        void SetParticipantPrepareTimestamp(uint64_t transaction_id, const Timestamp &prepare_ts);
        TransactionState FinishParticipantPrepare(uint64_t transaction_id);

        void AbortPrepare(uint64_t transaction_id);
        void PausePrepare(uint64_t transaction_id);
        TransactionState ContinuePrepare(uint64_t transaction_id);

        TransactionState CoordinatorReceivePrepareOK(uint64_t transaction_id, int participant_shard,
                                                     const Timestamp &prepare_ts,
                                                     const Timestamp &nonblock_ts);

        TransactionState ParticipantReceivePrepareOK(uint64_t transaction_id);

        TransactionFinishResult Commit(uint64_t transaction_id);
        TransactionFinishResult Abort(uint64_t transaction_id);

        Stats &GetStats() { return stats_; };

    private:
        class PendingRWTransaction
        {
        public:
            PendingRWTransaction() : client_addr_{nullptr}, coordinator_{-1}, state_{READING}, wait_start_{0} {}
            ~PendingRWTransaction() {}

            TransactionState state() const { return state_; }
            void set_state(TransactionState s) { state_ = s; }

            const Timestamp &nonblock_ts() const { return nonblock_ts_; }
            void advance_nonblock_ts(uint64_t d) { nonblock_ts_.setTimestamp(nonblock_ts_.getTimestamp() + d); }

            const Timestamp &start_ts() const { return start_ts_; }
            const Timestamp &prepare_ts() const { return prepare_ts_; }
            const Timestamp &commit_ts() const { return commit_ts_; }
            const Transaction &transaction() const { return transaction_; }
            int coordinator() const { return coordinator_; }
            std::shared_ptr<TransportAddress> client_addr() const { return client_addr_; }

            const std::unordered_set<int> &participants() const { return participants_; }

            const std::unordered_set<uint64_t> &waiting_ros() const { return waiting_ros_; }
            void add_waiting_ro(uint64_t transaction_id)
            {
                waiting_ros_.insert(transaction_id);
            }

            const std::unordered_set<uint64_t> &slow_path_ros() const { return slow_path_ros_; }
            void add_slow_path_ro(uint64_t transaction_id)
            {
                slow_path_ros_.insert(transaction_id);
            }

            const uint64_t wait_start() const { return wait_start_; }
            void set_wait_start(uint64_t w) { wait_start_ = w; }

            void StartGet(const TransportAddress &remote, const std::string &key, bool for_update);

            void StartCoordinatorPrepare(const Timestamp &start_ts, int coordinator,
                                         const std::unordered_set<int> participants,
                                         const Transaction &transaction,
                                         const Timestamp &nonblock_ts);

            void FinishCoordinatorPrepare(const Timestamp &prepare_ts);

            void ReceivePrepareOK(int coordinator, int participant,
                                  const Timestamp &prepare_ts,
                                  const Timestamp &nonblock_ts);

            void StartParticipantPrepare(int coordinator,
                                         const Transaction &transaction,
                                         const Timestamp &nonblock_ts);
            void SetParticipantPrepareTimestamp(const Timestamp &prepare_ts);
            void FinishParticipantPrepare();

        private:
            Transaction transaction_;
            std::unordered_set<int> participants_;
            std::unordered_set<int> ok_participants_;
            std::unordered_set<uint64_t> waiting_ros_;
            std::unordered_set<uint64_t> slow_path_ros_;
            Timestamp start_ts_;
            Timestamp nonblock_ts_;
            Timestamp prepare_ts_;
            Timestamp commit_ts_;
            std::shared_ptr<TransportAddress> client_addr_;
            int coordinator_;
            TransactionState state_;
            uint64_t wait_start_;
        };

        class PendingROTransaction
        {
        public:
            PendingROTransaction() : state_{PREPARING} {}
            ~PendingROTransaction() {}

            TransactionState state() const { return state_; }
            void set_state(TransactionState s) { state_ = s; }

            uint64_t n_conflicts() const { return n_conflicts_; }
            void decr_conflicts() { n_conflicts_ -= 1; }

            const std::unordered_set<uint64_t> &skipped_rws() const { return skipped_rws_; }
            void add_skipped_rw(uint64_t transaction_id) { skipped_rws_.insert(transaction_id); }

            const Timestamp &min_ts() const { return min_ts_; }
            const Timestamp &commit_ts() const { return commit_ts_; }
            const std::unordered_set<std::string> &keys() const { return keys_; }

            void StartRO(const std::unordered_set<std::string> &keys,
                         const Timestamp &min_ts,
                         const Timestamp &commit_ts,
                         uint64_t n_conflicts);

        private:
            std::unordered_set<std::string> keys_;
            std::unordered_set<uint64_t> skipped_rws_;
            Timestamp min_ts_;
            Timestamp commit_ts_;
            uint64_t n_conflicts_;
            TransactionState state_;
        };

        void NotifyROs(std::unordered_set<uint64_t> &ros);

        std::unordered_map<uint64_t, PendingRWTransaction> pending_rw_;
        std::unordered_map<uint64_t, PendingROTransaction> pending_ro_;
        std::unordered_set<uint64_t> committed_;
        std::unordered_set<uint64_t> aborted_;
        Stats stats_;
        int this_shard_;
        Consistency consistency_;
        const TrueTime &tt_;
    };

} // namespace strongstore

#endif /* _STRONG_TRANSACTION_STORE_H_ */
