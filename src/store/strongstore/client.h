/***********************************************************************
 *
 * store/strongstore/client.h:
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
#ifndef _STRONG_CLIENT_H_
#define _STRONG_CLIENT_H_

#include <bitset>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "lib/udptransport.h"
#include "replication/vr/client.h"
#include "store/common/frontend/client.h"
#include "store/common/partitioner.h"
#include "store/common/truetime.h"
#include "store/strongstore/common.h"
#include "store/strongstore/networkconfig.h"
#include "store/strongstore/preparedtransaction.h"
#include "store/strongstore/shardclient.h"
#include "store/strongstore/strong-proto.pb.h"

namespace strongstore
{

    class StrongSession : public ::Session
    {
    public:
        StrongSession()
            : ::Session(), transaction_id_{static_cast<uint64_t>(-1)}, start_ts_{0, 0}, min_read_ts_{0, 0}, participants_{}, prepares_{}, values_{}, snapshot_ts_{}, current_participant_{-1}, state_{EXECUTING} {}

        StrongSession(rss::Session &&rss_session)
            : ::Session(std::move(rss_session)), transaction_id_{static_cast<uint64_t>(-1)}, start_ts_{0, 0}, min_read_ts_{0, 0}, participants_{}, prepares_{}, values_{}, snapshot_ts_{}, current_participant_{-1}, state_{EXECUTING} {}

        uint64_t transaction_id() const { return transaction_id_; }
        const Timestamp &start_ts() const { return start_ts_; }

        const Timestamp &min_read_ts() const { return min_read_ts_; }
        void advance_min_read_ts(const Timestamp &ts) { min_read_ts_ = std::max(min_read_ts_, ts); }

        const std::set<int> &participants() const { return participants_; }
        const std::unordered_map<uint64_t, PreparedTransaction> prepares() const { return prepares_; }

        const Timestamp &snapshot_ts() const { return snapshot_ts_; }

    protected:
        friend class Client;

        void start_transaction(uint64_t transaction_id, const Timestamp &start_ts)
        {
            transaction_id_ = transaction_id;
            start_ts_ = start_ts;
            participants_.clear();
            prepares_.clear();
            values_.clear();
            snapshot_ts_ = Timestamp();
            current_participant_ = -1;
            state_ = EXECUTING;
        }

        void retry_transaction(uint64_t transaction_id)
        {
            transaction_id_ = transaction_id;
            participants_.clear();
            prepares_.clear();
            values_.clear();
            snapshot_ts_ = Timestamp();
            current_participant_ = -1;
            state_ = EXECUTING;
        }

        enum State
        {
            EXECUTING = 0,
            GETTING,
            PUTTING,
            COMMITTING,
            NEEDS_ABORT,
            ABORTING
        };

        State state() const { return state_; }

        bool executing() const { return (state_ == EXECUTING); }
        bool needs_aborts() const { return (state_ == NEEDS_ABORT); }

        int current_participant() const { return current_participant_; }

        void set_executing()
        {
            current_participant_ = -1;
            state_ = EXECUTING;
        }

        void set_getting(int p)
        {
            current_participant_ = p;
            state_ = GETTING;
        }

        void set_putting(int p)
        {
            current_participant_ = p;
            state_ = PUTTING;
        }

        void set_committing() { state_ = COMMITTING; }
        void set_needs_abort() { state_ = NEEDS_ABORT; }
        void set_aborting() { state_ = ABORTING; }

        std::set<int> &mutable_participants() { return participants_; }
        void add_participant(int p) { participants_.insert(p); }
        void clear_participants() { participants_.clear(); }

        std::unordered_map<uint64_t, PreparedTransaction> &mutable_prepares() { return prepares_; }
        std::unordered_map<std::string, std::list<Value>> &mutable_values() { return values_; }

        void set_snapshot_ts(const Timestamp &ts) { snapshot_ts_ = ts; }

    private:
        uint64_t transaction_id_;
        Timestamp start_ts_;
        Timestamp min_read_ts_;
        std::set<int> participants_;
        std::unordered_map<uint64_t, PreparedTransaction> prepares_;
        std::unordered_map<std::string, std::list<Value>> values_;
        Timestamp snapshot_ts_;
        int current_participant_;
        State state_;
    };

    class CommittedTransaction
    {
    public:
        uint64_t transaction_id;
        Timestamp commit_ts;
        bool committed;
    };

    enum SnapshotState
    {
        WAIT,
        COMMIT
    };

    struct SnapshotResult
    {
        SnapshotState state;
        Timestamp max_read_ts;
        std::unordered_map<std::string, std::string> kv_;
    };

    class Client : public ::Client
    {
    public:
        Client(Consistency consistency, const NetworkConfiguration &net_config,
               const std::string &client_region, transport::Configuration &config,
               uint64_t id, int nshards, int closestReplic, Transport *transport,
               Partitioner *part, TrueTime &tt, bool debug_stats,
               double nb_time_alpha);
        virtual ~Client();

        virtual Session &BeginSession() override;
        virtual Session &ContinueSession(rss::Session &session) override;
        virtual rss::Session EndSession(Session &session) override;

        // Overriding functions from ::Client
        // Begin a transaction
        virtual void Begin(Session &session, begin_callback bcb, begin_timeout_callback btcb, uint32_t timeout) override;

        // Begin a retried transaction.
        virtual void Retry(Session &session, begin_callback bcb,
                           begin_timeout_callback btcb, uint32_t timeout) override;

        // Get the value corresponding to key.
        virtual void Get(Session &session, const std::string &key,
                         get_callback gcb, get_timeout_callback gtcb,
                         uint32_t timeout = GET_TIMEOUT) override;

        // Get the value corresponding to key.
        // Provide hint that transaction will later write the key.
        virtual void GetForUpdate(Session &session, const std::string &key,
                                  get_callback gcb, get_timeout_callback gtcb,
                                  uint32_t timeout = GET_TIMEOUT) override;

        // Set the value for the given key.
        virtual void Put(Session &session, const std::string &key, const std::string &value,
                         put_callback pcb, put_timeout_callback ptcb,
                         uint32_t timeout = PUT_TIMEOUT) override;

        // Commit all Get(s) and Put(s) since Begin().
        virtual void Commit(Session &session, commit_callback ccb, commit_timeout_callback ctcb,
                            uint32_t timeout) override;

        // Abort all Get(s) and Put(s) since Begin().
        virtual void Abort(Session &session, abort_callback acb, abort_timeout_callback atcb,
                           uint32_t timeout) override;
        // Force transaction to abort.
        void ForceAbort(const uint64_t transaction_id) override;

        // Commit all Get(s) and Put(s) since Begin().
        void ROCommit(Session &session, const std::unordered_set<std::string> &keys,
                      commit_callback ccb, commit_timeout_callback ctcb,
                      uint32_t timeout) override;

    private:
        const static std::size_t MAX_SHARDS = 16;

        struct PendingRequest
        {
            PendingRequest(uint64_t id)
                : id(id), outstandingPrepares(0) {}

            ~PendingRequest() {}

            commit_callback ccb;
            commit_timeout_callback ctcb;
            abort_callback acb;
            abort_timeout_callback atcb;
            uint64_t id;
            int outstandingPrepares;
        };

        void ContinueBegin(Session &session, begin_callback bcb);
        void ContinueRetry(Session &session, begin_callback bcb);

        // local Prepare function
        void CommitCallback(StrongSession &session, uint64_t req_id, int status, Timestamp commit_ts, Timestamp nonblock_ts);

        void AbortCallback(StrongSession &session, uint64_t req_id);

        void ROCommitCallback(StrongSession &session, uint64_t req_id, int shard_idx,
                              const std::vector<Value> &values,
                              const std::vector<PreparedTransaction> &prepares);

        void ROCommitSlowCallback(StrongSession &session, uint64_t req_id, int shard_idx,
                                  uint64_t rw_transaction_id, const Timestamp &commit_ts, bool is_commit);

        void HandleWound(const uint64_t transaction_id);

        void RealTimeBarrier(const rss::Session &session, rss::continuation_func_t continuation);

        // choose coordinator from participants
        void CalculateCoordinatorChoices();
        int ChooseCoordinator(StrongSession &session);

        // Choose nonblock time
        Timestamp ChooseNonBlockTimestamp(StrongSession &session);

        // For tracking RO reply progress
        SnapshotResult ReceiveFastPath(StrongSession &session, uint64_t transaction_id,
                                       int shard_idx,
                                       const std::vector<Value> &values,
                                       const std::vector<PreparedTransaction> &prepares);
        SnapshotResult ReceiveSlowPath(StrongSession &session, uint64_t transaction_id,
                                       uint64_t rw_transaction_id,
                                       bool is_commit, const Timestamp &commit_ts);
        SnapshotResult FindSnapshot(std::unordered_map<uint64_t, PreparedTransaction> &prepared,
                                    std::vector<CommittedTransaction> &committed);
        void AddValues(StrongSession &session, const std::vector<Value> &values);
        void AddPrepares(StrongSession &session, const std::vector<PreparedTransaction> &prepares);
        void ReceivedAllFastPaths(StrongSession &session);
        void FindCommittedKeys(StrongSession &session);
        void CalculateSnapshotTimestamp(StrongSession &session);
        SnapshotResult CheckCommit(StrongSession &session);

        std::unordered_map<std::bitset<MAX_SHARDS>, int> coord_choices_;
        std::unordered_map<std::bitset<MAX_SHARDS>, uint16_t> min_lats_;

        std::unordered_map<uint64_t, StrongSession> sessions_;
        std::unordered_map<uint64_t, StrongSession &> sessions_by_transaction_id_;
        std::unordered_map<uint64_t, Timestamp> tmins_;

        const strongstore::NetworkConfiguration &net_config_;
        const std::string client_region_;

        const std::string service_name_;

        transport::Configuration &config_;

        // Unique ID for this client.
        uint64_t client_id_;

        // Number of shards in SpanStore.
        uint64_t nshards_;

        // Transport used by paxos client proxies.
        Transport *transport_;

        // Client for each shard.
        std::vector<ShardClient *> sclients_;

        // Partitioner
        Partitioner *part_;

        // TrueTime server.
        TrueTime &tt_;

        uint64_t next_transaction_id_;

        uint64_t last_req_id_;
        std::unordered_map<uint64_t, PendingRequest *> pending_reqs_;

        Latency_t op_lat_;
        Latency_t commit_lat_;

        Consistency consistency_;

        double nb_time_alpha_;

        bool debug_stats_;
    };

} // namespace strongstore

#endif /* _STRONG_CLIENT_H_ */
