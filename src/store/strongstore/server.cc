#include "store/strongstore/server.h"

#include <algorithm>
#include <functional>
#include <memory>
#include <unordered_set>

namespace strongstore {

using namespace std;
using namespace proto;
using namespace replication;

Server::Server(Consistency consistency,
               const transport::Configuration &shard_config,
               const transport::Configuration &replica_config,
               uint64_t server_id, int shard_idx, int replica_idx,
               Transport *transport, const TrueTime &tt, bool debug_stats)
    : PingServer(transport),
      tt_{tt},
      transactions_{shard_idx, consistency, tt_},
      shard_config_{shard_config},
      replica_config_{replica_config},
      transport_{transport},
      server_id_{server_id},
      min_prepare_timestamp_{},
      shard_idx_{shard_idx},
      replica_idx_{replica_idx},
      consistency_{consistency},
      debug_stats_{debug_stats} {
    transport_->Register(this, shard_config_, shard_idx_, replica_idx_);

    for (int i = 0; i < shard_config_.g; i++) {
        shard_clients_.push_back(new ShardClient(shard_config_, transport, server_id_, i));
    }

    replica_client_ =
        new ReplicaClient(replica_config_, transport_, server_id_, shard_idx_);

    if (debug_stats_) {
        _Latency_Init(&ro_wait_lat_, "ro_wait_lat");
    }
}

Server::~Server() {
    for (auto s : shard_clients_) {
        delete s;
    }

    delete replica_client_;

    if (debug_stats_) {
        Latency_Dump(&ro_wait_lat_);
    }
}

// Assume GetStats called once before exiting protgram
Stats &Server::GetStats() {
    Stats &s = transactions_.GetStats();
    stats_.Merge(s);
    return stats_;
}

void Server::ReceiveMessage(const TransportAddress &remote,
                            const std::string &type, const std::string &data,
                            void *meta_data) {
    if (type == get_.GetTypeName()) {
        get_.ParseFromString(data);
        HandleGet(remote, get_);
    } else if (type == rw_commit_c_.GetTypeName()) {
        rw_commit_c_.ParseFromString(data);
        HandleRWCommitCoordinator(remote, rw_commit_c_);
    } else if (type == rw_commit_p_.GetTypeName()) {
        rw_commit_p_.ParseFromString(data);
        HandleRWCommitParticipant(remote, rw_commit_p_);
    } else if (type == prepare_ok_.GetTypeName()) {
        prepare_ok_.ParseFromString(data);
        HandlePrepareOK(remote, prepare_ok_);
    } else if (type == prepare_abort_.GetTypeName()) {
        prepare_abort_.ParseFromString(data);
        HandlePrepareAbort(remote, prepare_abort_);
    } else if (type == ro_commit_.GetTypeName()) {
        ro_commit_.ParseFromString(data);
        HandleROCommit(remote, ro_commit_);
    } else if (type == abort_.GetTypeName()) {
        abort_.ParseFromString(data);
        HandleAbort(remote, abort_);
    } else if (type == wound_.GetTypeName()) {
        wound_.ParseFromString(data);
        HandleWound(remote, wound_);
    } else if (type == ping_.GetTypeName()) {
        ping_.ParseFromString(data);
        HandlePingMessage(this, remote, ping_);
    } else {
        Panic("Received unexpected message type: %s", type.c_str());
    }
}

void Server::HandleGet(const TransportAddress &remote, proto::Get &msg) {
    uint64_t client_id = msg.rid().client_id();
    uint64_t client_req_id = msg.rid().client_req_id();
    uint64_t transaction_id = msg.transaction_id();

    const std::string &key = msg.key();
    const Timestamp timestamp{msg.timestamp()};

    bool for_update = msg.has_for_update() && msg.for_update();

    Debug("[%lu] Received GET request: %s %d", transaction_id, key.c_str(), for_update);

    transactions_.StartGet(transaction_id, remote, key, for_update);

    LockAcquireResult r;
    if (for_update) {
        r = locks_.AcquireReadWriteLock(transaction_id, timestamp, key);
    } else {
        r = locks_.AcquireReadLock(transaction_id, timestamp, key);
    }

    if (r.status == LockStatus::ACQUIRED) {
        ASSERT(r.wound_rws.size() == 0);

        std::pair<TimestampID, std::string> value;
        ASSERT(store_.get(key, value));

        get_reply_.Clear();
        get_reply_.mutable_rid()->CopyFrom(msg.rid());
        get_reply_.set_status(REPLY_OK);
        get_reply_.set_key(msg.key());

        get_reply_.set_val(value.second);
        value.first.timestamp.serialize(get_reply_.mutable_timestamp());

        transport_->SendMessage(this, remote, get_reply_);

        transactions_.FinishGet(transaction_id, key);
    } else if (r.status == LockStatus::FAIL) {
        ASSERT(r.wound_rws.size() == 0);

        get_reply_.Clear();
        get_reply_.mutable_rid()->CopyFrom(msg.rid());
        get_reply_.set_status(REPLY_FAIL);
        get_reply_.set_key(msg.key());

        transport_->SendMessage(this, remote, get_reply_);

        const Transaction &transaction = transactions_.GetTransaction(transaction_id);

        LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);
        transactions_.AbortGet(transaction_id, key);

        NotifyPendingRWs(transaction_id, rr.notify_rws);
    } else if (r.status == LockStatus::WAITING) {
        auto reply = new PendingGetReply(client_id, client_req_id, remote.clone());
        reply->key = key;

        pending_get_replies_[msg.transaction_id()] = reply;

        transactions_.PauseGet(transaction_id, key);

        WoundPendingRWs(transaction_id, r.wound_rws);
    } else {
        NOT_REACHABLE();
    }
}

void Server::ContinueGet(uint64_t transaction_id) {
    auto search = pending_get_replies_.find(transaction_id);
    if (search == pending_get_replies_.end()) {
        return;
    }

    PendingGetReply *reply = search->second;

    uint64_t client_id = reply->rid.client_id();
    uint64_t client_req_id = reply->rid.client_req_id();
    const TransportAddress *remote = reply->rid.addr();

    const std::string &key = reply->key;

    Debug("[%lu] Continuing GET request %s", transaction_id, key.c_str());

    get_reply_.Clear();
    get_reply_.mutable_rid()->set_client_id(client_id);
    get_reply_.mutable_rid()->set_client_req_id(client_req_id);
    get_reply_.set_key(key);

    TransactionState s = transactions_.ContinueGet(transaction_id, key);
    if (s == READING) {
        ASSERT(locks_.HasReadLock(transaction_id, key));

        std::pair<TimestampID, std::string> value;
        ASSERT(store_.get(key, value));

        get_reply_.set_status(REPLY_OK);
        get_reply_.set_val(value.second);

        value.first.timestamp.serialize(get_reply_.mutable_timestamp());

        transport_->SendMessage(this, *remote, get_reply_);

        transactions_.FinishGet(transaction_id, key);
    } else if (s == ABORTED) {  // Already aborted
        get_reply_.set_status(REPLY_FAIL);
        transport_->SendMessage(this, *remote, get_reply_);
    } else {
        NOT_REACHABLE();
    }

    delete remote;
    delete reply;
    pending_get_replies_.erase(search);
}

const Timestamp Server::GetPrepareTimestamp(uint64_t client_id) {
    uint64_t ts = std::max(tt_.Now().earliest(), min_prepare_timestamp_.getTimestamp() + 1);
    const Timestamp prepare_timestamp{ts, client_id};
    min_prepare_timestamp_ = prepare_timestamp;

    return prepare_timestamp;
}

void Server::WoundPendingRWs(uint64_t transaction_id, const std::unordered_set<uint64_t> &rws) {
    for (uint64_t rw : rws) {
        ASSERT(transaction_id != rw);
        //Debug("[%lu] Wounding %lu", transaction_id, rw);
        TransactionState s = transactions_.GetRWTransactionState(rw);
        ASSERT(s != NOT_FOUND);

        if (s == READING || s == READ_WAIT) {
            // Send wound to client
            std::shared_ptr<TransportAddress> remote = transactions_.GetClientAddr(rw);
            wound_.set_transaction_id(rw);
            transport_->SendMessage(this, *remote, wound_);
        } else if (s == PREPARING || s == WAIT_PARTICIPANTS || s == PREPARE_WAIT || s == PREPARED) {
            // Send wound to coordinator
            int coordinator = transactions_.GetCoordinator(rw);
            ASSERT(coordinator >= 0);
            shard_clients_[coordinator]->Wound(rw);

        } else if (s == COMMITTING || s == COMMITTED || s == ABORTED) {
            //Debug("[%lu] Not wounding. Will complete soon", rw);
        } else {
            NOT_REACHABLE();
        }
    }
}

void Server::NotifyPendingRWs(uint64_t transaction_id, const std::unordered_set<uint64_t> &rws) {
    for (uint64_t waiting_rw : rws) {
        if (transaction_id != waiting_rw) {
            Debug("[%lu] continuing %lu", transaction_id, waiting_rw);
            ContinueGet(waiting_rw);
            ContinueCoordinatorPrepare(waiting_rw);
            ContinueParticipantPrepare(waiting_rw);
        }
    }
}

void Server::NotifyPendingROs(const std::unordered_set<uint64_t> &ros) {
    for (uint64_t waiting_ro : ros) {
        ContinueROCommit(waiting_ro);
    }
}

void Server::NotifySlowPathROs(const std::unordered_set<uint64_t> &ros, uint64_t rw_transaction_id,
                               bool is_commit, const Timestamp &commit_ts) {
    for (uint64_t ro : ros) {
        SendROSlowPath(ro, rw_transaction_id, is_commit, commit_ts);
    }
}

void Server::SendROSlowPath(uint64_t ro_transaction_id, uint64_t rw_transaction_id,
                            bool is_commit, const Timestamp &commit_ts) {
    ASSERT(consistency_ == RSS);
    auto search = pending_ro_commit_replies_.find(ro_transaction_id);
    ASSERT(search != pending_ro_commit_replies_.end());

    //Debug("[%lu] Sending slow path reply for %lu", ro_transaction_id, rw_transaction_id);

    PendingROCommitReply *reply = search->second;
    ASSERT(reply->n_slow_path_replies > 0);

    if (transactions_.GetROTransactionState(ro_transaction_id) != SLOW_PATH) {
        //Debug("[%lu] Fast path reply not yet sent", ro_transaction_id);
        //Debug("s: %d", static_cast<int>(transactions_.GetROTransactionState(ro_transaction_id)));
        reply->n_slow_path_replies -= 1;
        return;
    }

    uint64_t client_id = reply->rid.client_id();
    uint64_t client_req_id = reply->rid.client_req_id();
    const TransportAddress *remote = reply->rid.addr();

    ro_commit_slow_reply_.mutable_rid()->set_client_id(client_id);
    ro_commit_slow_reply_.mutable_rid()->set_client_req_id(client_req_id);
    ro_commit_slow_reply_.set_transaction_id(rw_transaction_id);
    ro_commit_slow_reply_.set_is_commit(is_commit);
    commit_ts.serialize(ro_commit_slow_reply_.mutable_commit_timestamp());

    transport_->SendMessage(this, *remote, ro_commit_slow_reply_);

    uint64_t n_slow_path_replies = reply->n_slow_path_replies - 1;
    if (n_slow_path_replies == 0) {
        delete remote;
        delete reply;
        pending_ro_commit_replies_.erase(search);

        transactions_.FinishROSlowPath(ro_transaction_id);
    } else {
        reply->n_slow_path_replies = n_slow_path_replies;
    }
}

void Server::HandleROCommit(const TransportAddress &remote, proto::ROCommit &msg) {
    uint64_t client_id = msg.rid().client_id();
    uint64_t client_req_id = msg.rid().client_req_id();
    uint64_t transaction_id = msg.transaction_id();

    //Debug("[%lu] Received ROCommit request", transaction_id);

    std::unordered_set<std::string> keys{msg.keys().begin(), msg.keys().end()};

    const Timestamp commit_ts{msg.commit_timestamp()};
    const Timestamp min_ts{msg.min_timestamp()};

    min_prepare_timestamp_ = std::max(min_prepare_timestamp_, commit_ts);  // TODO: is this correct?

    TransactionState s = transactions_.StartRO(transaction_id, keys, min_ts, commit_ts);
    if (s == PREPARE_WAIT) {
        //Debug("[%lu] Waiting for prepared transactions", transaction_id);
        auto reply = new PendingROCommitReply(client_id, client_req_id, remote.clone());
        reply->n_slow_path_replies = transactions_.GetRONumberSkipped(transaction_id);
        pending_ro_commit_replies_[transaction_id] = reply;

        if (debug_stats_) {
            _Latency_StartRec(&reply->wait_lat);
        }

        return;
    }

    ro_commit_reply_.Clear();
    ro_commit_reply_.mutable_rid()->set_client_id(client_id);
    ro_commit_reply_.mutable_rid()->set_client_req_id(client_req_id);
    ro_commit_reply_.set_transaction_id(transaction_id);

    std::pair<TimestampID, std::string> value;
    for (auto &k : keys) {
        ASSERT(store_.get(k, {commit_ts, transaction_id}, value));
        proto::ReadReply *rreply = ro_commit_reply_.add_values();
        rreply->set_transaction_id(value.first.transaction_id);
        value.first.timestamp.serialize(rreply->mutable_timestamp());
        rreply->set_key(k.c_str());
        rreply->set_val(value.second.c_str());
    }

    if (consistency_ == RSS && transactions_.GetRONumberSkipped(transaction_id) > 0) {
        const std::vector<PreparedTransaction> skipped_prepares = transactions_.GetROSkippedRWTransactions(transaction_id);

        // Add for slow replies
        auto reply = new PendingROCommitReply(client_id, client_req_id, remote.clone());
        reply->n_slow_path_replies = skipped_prepares.size();
        pending_ro_commit_replies_[transaction_id] = reply;

        for (auto &pt : skipped_prepares) {
            //Debug("[%lu] replying with skipped prepare: %lu", transaction_id, pt.transaction_id());
            proto::PreparedTransactionMessage *ptm = ro_commit_reply_.add_prepares();
            pt.serialize(ptm);
        }

        transport_->SendMessage(this, remote, ro_commit_reply_);

        transactions_.StartROSlowPath(transaction_id);
    } else {
        transport_->SendMessage(this, remote, ro_commit_reply_);

        transactions_.CommitRO(transaction_id);
    }
}

void Server::ContinueROCommit(uint64_t transaction_id) {
    auto search = pending_ro_commit_replies_.find(transaction_id);
    ASSERT(search != pending_ro_commit_replies_.end());

    PendingROCommitReply *reply = search->second;

    uint64_t client_id = reply->rid.client_id();
    uint64_t client_req_id = reply->rid.client_req_id();
    const TransportAddress *remote = reply->rid.addr();

    //Debug("[%lu] Continuing RO commit", transaction_id);

    transactions_.ContinueRO(transaction_id);

    const Timestamp &commit_ts = transactions_.GetROCommitTimestamp(transaction_id);
    const std::unordered_set<std::string> &keys = transactions_.GetROKeys(transaction_id);

    ro_commit_reply_.Clear();
    ro_commit_reply_.mutable_rid()->set_client_id(client_id);
    ro_commit_reply_.mutable_rid()->set_client_req_id(client_req_id);
    ro_commit_reply_.set_transaction_id(transaction_id);

    std::pair<TimestampID, std::string> value;
    for (auto &k : keys) {
        ASSERT(store_.get(k, {commit_ts, transaction_id}, value));
        proto::ReadReply *rreply = ro_commit_reply_.add_values();
        rreply->set_transaction_id(value.first.transaction_id);
        value.first.timestamp.serialize(rreply->mutable_timestamp());
        rreply->set_key(k.c_str());
        rreply->set_val(value.second.c_str());
    }

    if (consistency_ == RSS && transactions_.GetRONumberSkipped(transaction_id) > 0) {
        const std::vector<PreparedTransaction> skipped_prepares = transactions_.GetROSkippedRWTransactions(transaction_id);
        // Add for slow replies
        reply->n_slow_path_replies = skipped_prepares.size();

        for (auto &pt : skipped_prepares) {
            //Debug("[%lu] replying with skipped prepare: %lu", transaction_id, pt.transaction_id());
            proto::PreparedTransactionMessage *ptm = ro_commit_reply_.add_prepares();
            pt.serialize(ptm);
        }

        transport_->SendMessage(this, *remote, ro_commit_reply_);

        transactions_.StartROSlowPath(transaction_id);
    } else {
        transport_->SendMessage(this, *remote, ro_commit_reply_);

        delete remote;
        delete reply;
        pending_ro_commit_replies_.erase(search);

        transactions_.CommitRO(transaction_id);
    }
}

void Server::HandleRWCommitCoordinator(const TransportAddress &remote, proto::RWCommitCoordinator &msg) {
    uint64_t client_id = msg.rid().client_id();
    uint64_t client_req_id = msg.rid().client_req_id();

    uint64_t transaction_id = msg.transaction_id();

    std::unordered_set<int> participants{msg.participants().begin(),
                                         msg.participants().end()};

    const Transaction transaction{msg.transaction()};
    const Timestamp nonblock_ts{msg.nonblock_timestamp()};

    Debug("[%lu] Coordinator for transaction", transaction_id);

    const TrueTimeInterval now = tt_.Now();
    const Timestamp start_ts{now.latest(), client_id};
    TransactionState s = transactions_.StartCoordinatorPrepare(transaction_id, start_ts, shard_idx_,
                                                               participants, transaction, nonblock_ts);

    if (s == PREPARING) {
        //Debug("[%lu] Coordinator preparing", transaction_id);

        LockAcquireResult ar = locks_.AcquireLocks(transaction_id, transaction);
        if (ar.status == LockStatus::ACQUIRED) {
            ASSERT(ar.wound_rws.size() == 0);
            const Timestamp prepare_ts = GetPrepareTimestamp(client_id);
            transactions_.FinishCoordinatorPrepare(transaction_id, prepare_ts);
            const Timestamp &commit_ts = transactions_.GetRWCommitTimestamp(transaction_id);

            auto *reply = new PendingRWCommitCoordinatorReply(client_id, client_req_id, remote.clone());
            pending_rw_commit_c_replies_[transaction_id] = reply;

            // TODO: Handle timeout
            replica_client_->CoordinatorCommit(
                transaction_id, start_ts, shard_idx_,
                participants, transaction, nonblock_ts, commit_ts,
                std::bind(&Server::CommitCoordinatorCallback, this,
                          transaction_id, std::placeholders::_1),
                []() {}, COMMIT_TIMEOUT);

        } else if (ar.status == LockStatus::FAIL) {
            ASSERT(ar.wound_rws.size() == 0);
            //Debug("[%lu] Coordinator prepare failed", transaction_id);
            LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);

            SendRWCommmitCoordinatorReplyFail(remote, client_id, client_req_id);

            NotifyPendingRWs(transaction_id, rr.notify_rws);

            transactions_.AbortPrepare(transaction_id);
        } else if (ar.status == LockStatus::WAITING) {
            Debug("[%lu] Waiting", transaction_id);

            auto reply = new PendingRWCommitCoordinatorReply(client_id, client_req_id, remote.clone());
            pending_rw_commit_c_replies_[transaction_id] = reply;

            transactions_.PausePrepare(transaction_id);

            WoundPendingRWs(transaction_id, ar.wound_rws);
        } else {
            NOT_REACHABLE();
        }

    } else if (s == ABORTED) {
        //Debug("[%lu] Already aborted", transaction_id);

        SendRWCommmitCoordinatorReplyFail(remote, client_id, client_req_id);

        SendAbortParticipants(transaction_id, participants);

    } else if (s == WAIT_PARTICIPANTS) {
        //Debug("[%lu] Waiting for other participants", transaction_id);

        auto reply = new PendingRWCommitCoordinatorReply(client_id, client_req_id, remote.clone());
        pending_rw_commit_c_replies_[transaction_id] = reply;
    } else {
        NOT_REACHABLE();
    }
}

void Server::ContinueCoordinatorPrepare(uint64_t transaction_id) {
    auto search = pending_rw_commit_c_replies_.find(transaction_id);
    if (search == pending_rw_commit_c_replies_.end()) {
        return;
    }
    Debug("[%lu] Cont coord prep", transaction_id);

    PendingRWCommitCoordinatorReply *reply = search->second;

    uint64_t client_id = reply->rid.client_id();
    uint64_t client_req_id = reply->rid.client_req_id();
    const TransportAddress *remote = reply->rid.addr();

    TransactionState s = transactions_.ContinuePrepare(transaction_id);
    if (s == PREPARING) {
        const Transaction &transaction = transactions_.GetTransaction(transaction_id);
        LockAcquireResult ar = locks_.AcquireLocks(transaction_id, transaction);
        if (ar.status == LockStatus::ACQUIRED) {
            ASSERT(ar.wound_rws.size() == 0);
            const Timestamp prepare_ts = GetPrepareTimestamp(client_id);
            transactions_.FinishCoordinatorPrepare(transaction_id, prepare_ts);
            const Timestamp &commit_ts = transactions_.GetRWCommitTimestamp(transaction_id);

            const Timestamp &start_ts = transactions_.GetStartTimestamp(transaction_id);
            const std::unordered_set<int> &participants = transactions_.GetParticipants(transaction_id);
            const Timestamp &nonblock_ts = transactions_.GetNonBlockTimestamp(transaction_id);

            // TODO: Handle timeout
            replica_client_->CoordinatorCommit(
                transaction_id, start_ts, shard_idx_,
                participants, transaction, nonblock_ts, commit_ts,
                std::bind(&Server::CommitCoordinatorCallback, this,
                          transaction_id, std::placeholders::_1),
                []() {}, COMMIT_TIMEOUT);

        } else if (ar.status == LockStatus::FAIL) {
            ASSERT(ar.wound_rws.size() == 0);
            //Debug("[%lu] Coordinator prepare failed", transaction_id);
            LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);

            SendRWCommmitCoordinatorReplyFail(*remote, client_id, client_req_id);
            delete remote;
            delete reply;
            pending_rw_commit_c_replies_.erase(search);

            NotifyPendingRWs(transaction_id, rr.notify_rws);

            transactions_.AbortPrepare(transaction_id);
        } else if (ar.status == LockStatus::WAITING) {
            Debug("[%lu] Waiting", transaction_id);

            transactions_.PausePrepare(transaction_id);

            WoundPendingRWs(transaction_id, ar.wound_rws);
        } else {
            NOT_REACHABLE();
        }
    } else if (s == PREPARED || s == COMMITTING || s == COMMITTED) {
        //Debug("[%lu] Already prepared", transaction_id);
    } else if (s == ABORTED) {  // Already aborted
        //Debug("[%lu] Already aborted", transaction_id);
    } else {
        NOT_REACHABLE();
    }
}

void Server::SendRWCommmitCoordinatorReplyOK(uint64_t transaction_id,
                                             const Timestamp &commit_ts,
                                             const Timestamp &nonblock_ts) {
    auto search = pending_rw_commit_c_replies_.find(transaction_id);
    if (search == pending_rw_commit_c_replies_.end()) {
        //Debug("[%lu] No pending commit coordinator reply found", transaction_id);
        return;
    }

    PendingRWCommitCoordinatorReply *reply = search->second;

    uint64_t client_id = reply->rid.client_id();
    uint64_t client_req_id = reply->rid.client_req_id();
    const TransportAddress *remote = reply->rid.addr();

    rw_commit_c_reply_.mutable_rid()->set_client_id(client_id);
    rw_commit_c_reply_.mutable_rid()->set_client_req_id(client_req_id);
    rw_commit_c_reply_.set_status(REPLY_OK);
    commit_ts.serialize(rw_commit_c_reply_.mutable_commit_timestamp());
    nonblock_ts.serialize(rw_commit_c_reply_.mutable_nonblock_timestamp());

    transport_->SendMessage(this, *remote, rw_commit_c_reply_);

    delete remote;
    delete reply;
    pending_rw_commit_c_replies_.erase(search);
}

void Server::SendRWCommmitCoordinatorReplyFail(const TransportAddress &remote,
                                               uint64_t client_id,
                                               uint64_t client_req_id) {
    rw_commit_c_reply_.mutable_rid()->set_client_id(client_id);
    rw_commit_c_reply_.mutable_rid()->set_client_req_id(client_req_id);
    rw_commit_c_reply_.set_status(REPLY_FAIL);
    rw_commit_c_reply_.clear_commit_timestamp();
    rw_commit_c_reply_.clear_nonblock_timestamp();

    transport_->SendMessage(this, remote, rw_commit_c_reply_);
}

void Server::SendPrepareOKRepliesOK(uint64_t transaction_id, const Timestamp &commit_ts) {
    auto search = pending_prepare_ok_replies_.find(transaction_id);
    if (search == pending_prepare_ok_replies_.end()) {
        //Debug("[%lu] No pending prepare ok reply found", transaction_id);
        return;
    }
    PendingPrepareOKReply *reply = search->second;

    prepare_ok_reply_.set_status(REPLY_OK);
    commit_ts.serialize(prepare_ok_reply_.mutable_commit_timestamp());

    for (auto &rid : reply->rids) {
        uint64_t client_id = rid.client_id();
        uint64_t client_req_id = rid.client_req_id();
        const TransportAddress *remote = rid.addr();

        prepare_ok_reply_.mutable_rid()->set_client_id(client_id);
        prepare_ok_reply_.mutable_rid()->set_client_req_id(client_req_id);

        transport_->SendMessage(this, *remote, prepare_ok_reply_);
        delete remote;
    }

    delete reply;
    pending_prepare_ok_replies_.erase(search);
}

void Server::SendPrepareOKRepliesFail(PendingPrepareOKReply *reply) {
    prepare_ok_reply_.set_status(REPLY_FAIL);
    prepare_ok_reply_.clear_commit_timestamp();

    for (auto &rid : reply->rids) {
        uint64_t client_id = rid.client_id();
        uint64_t client_req_id = rid.client_req_id();
        const TransportAddress *remote = rid.addr();

        prepare_ok_reply_.mutable_rid()->set_client_id(client_id);
        prepare_ok_reply_.mutable_rid()->set_client_req_id(client_req_id);

        transport_->SendMessage(this, *remote, prepare_ok_reply_);
        delete remote;
    }
}

void Server::CommitCoordinatorCallback(uint64_t transaction_id, transaction_status_t status) {
    ASSERT(status == REPLY_OK);

    //Debug("[%lu] COMMIT callback: %d", transaction_id, status);
}

void Server::SendRWCommmitParticipantReplyOK(uint64_t transaction_id) {
    auto search = pending_rw_commit_p_replies_.find(transaction_id);
    ASSERT(search != pending_rw_commit_p_replies_.end());

    PendingRWCommitParticipantReply *reply = search->second;

    uint64_t client_id = reply->rid.client_id();
    uint64_t client_req_id = reply->rid.client_req_id();
    const TransportAddress *remote = reply->rid.addr();

    rw_commit_p_reply_.mutable_rid()->set_client_id(client_id);
    rw_commit_p_reply_.mutable_rid()->set_client_req_id(client_req_id);
    rw_commit_p_reply_.set_status(REPLY_OK);

    transport_->SendMessage(this, *remote, rw_commit_p_reply_);

    delete remote;
    delete reply;
    pending_rw_commit_p_replies_.erase(search);
}

void Server::SendRWCommmitParticipantReplyFail(uint64_t transaction_id) {
    auto search = pending_rw_commit_p_replies_.find(transaction_id);
    ASSERT(search != pending_rw_commit_p_replies_.end());

    PendingRWCommitParticipantReply *reply = search->second;

    uint64_t client_id = reply->rid.client_id();
    uint64_t client_req_id = reply->rid.client_req_id();
    const TransportAddress *remote = reply->rid.addr();

    rw_commit_p_reply_.mutable_rid()->set_client_id(client_id);
    rw_commit_p_reply_.mutable_rid()->set_client_req_id(client_req_id);
    rw_commit_p_reply_.set_status(REPLY_FAIL);

    transport_->SendMessage(this, *remote, rw_commit_p_reply_);

    delete remote;
    delete reply;
    pending_rw_commit_p_replies_.erase(search);
}

void Server::SendRWCommmitParticipantReplyFail(const TransportAddress &remote,
                                               uint64_t client_id,
                                               uint64_t client_req_id) {
    rw_commit_p_reply_.mutable_rid()->set_client_id(client_id);
    rw_commit_p_reply_.mutable_rid()->set_client_req_id(client_req_id);
    rw_commit_p_reply_.set_status(REPLY_FAIL);

    transport_->SendMessage(this, remote, rw_commit_p_reply_);
}

void Server::HandleRWCommitParticipant(const TransportAddress &remote, proto::RWCommitParticipant &msg) {
    uint64_t client_id = msg.rid().client_id();
    uint64_t client_req_id = msg.rid().client_req_id();

    uint64_t transaction_id = msg.transaction_id();
    int coordinator = msg.coordinator_shard();

    const Transaction transaction{msg.transaction()};
    const Timestamp nonblock_ts{msg.nonblock_timestamp()};

    Debug("[%lu] Participant for transaction", transaction_id);

    TransactionState s = transactions_.StartParticipantPrepare(transaction_id, coordinator, transaction, nonblock_ts);
    if (s == PREPARING) {
        LockAcquireResult ar = locks_.AcquireLocks(transaction_id, transaction);
        if (ar.status == LockStatus::ACQUIRED) {
            ASSERT(ar.wound_rws.size() == 0);
            const Timestamp prepare_ts = GetPrepareTimestamp(client_id);

            transactions_.SetParticipantPrepareTimestamp(transaction_id, prepare_ts);

            auto reply = new PendingRWCommitParticipantReply(client_id, client_req_id, remote.clone());
            pending_rw_commit_p_replies_[transaction_id] = reply;

            // TODO: Handle timeout
            replica_client_->Prepare(
                transaction_id, transaction, prepare_ts,
                coordinator, nonblock_ts,
                std::bind(&Server::PrepareCallback, this, transaction_id,
                          std::placeholders::_1, std::placeholders::_2),
                [](int, Timestamp) {}, PREPARE_TIMEOUT);
        } else if (ar.status == LockStatus::FAIL) {
            ASSERT(ar.wound_rws.size() == 0);
            //Debug("[%lu] Participant prepare failed", transaction_id);
            LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);

            // TODO: Handle timeout
            shard_clients_[coordinator]->PrepareAbort(
                transaction_id, shard_idx_,
                std::bind(&Server::PrepareAbortCallback, this, transaction_id,
                          placeholders::_1, placeholders::_2),
                [](int, Timestamp) {}, PREPARE_TIMEOUT);

            // Reply to client
            SendRWCommmitParticipantReplyFail(remote, client_id, client_req_id);

            NotifyPendingRWs(transaction_id, rr.notify_rws);

            transactions_.AbortPrepare(transaction_id);
        } else if (ar.status == LockStatus::WAITING) {
            Debug("[%lu] Waiting", transaction_id);

            auto reply = new PendingRWCommitParticipantReply(client_id, client_req_id, remote.clone());
            pending_rw_commit_p_replies_[transaction_id] = reply;

            transactions_.PausePrepare(transaction_id);

            WoundPendingRWs(transaction_id, ar.wound_rws);
        } else {
            NOT_REACHABLE();
        }
    } else if (s == ABORTED) {
        //Debug("[%lu] Already aborted", transaction_id);

        // Reply to client
        SendRWCommmitParticipantReplyFail(remote, client_id, client_req_id);

    } else {
        NOT_REACHABLE();
    }
}

void Server::ContinueParticipantPrepare(uint64_t transaction_id) {
    auto search = pending_rw_commit_p_replies_.find(transaction_id);
    if (search == pending_rw_commit_p_replies_.end()) {
        return;
    }

    Debug("[%lu] Cont part prep", transaction_id);
    PendingRWCommitParticipantReply *reply = search->second;

    uint64_t client_id = reply->rid.client_id();
    uint64_t client_req_id = reply->rid.client_req_id();
    const TransportAddress *remote = reply->rid.addr();

    TransactionState s = transactions_.ContinuePrepare(transaction_id);
    if (s == PREPARING) {
        const int coordinator = transactions_.GetCoordinator(transaction_id);
        const Transaction &transaction = transactions_.GetTransaction(transaction_id);

        LockAcquireResult ar = locks_.AcquireLocks(transaction_id, transaction);
        if (ar.status == LockStatus::ACQUIRED) {
            ASSERT(ar.wound_rws.size() == 0);
            const Timestamp prepare_ts = GetPrepareTimestamp(client_id);

            transactions_.SetParticipantPrepareTimestamp(transaction_id, prepare_ts);

            const Timestamp &nonblock_ts = transactions_.GetNonBlockTimestamp(transaction_id);

            // TODO: Handle timeout
            replica_client_->Prepare(
                transaction_id, transaction, prepare_ts,
                coordinator, nonblock_ts,
                std::bind(&Server::PrepareCallback, this, transaction_id,
                          std::placeholders::_1, std::placeholders::_2),
                [](int, Timestamp) {}, PREPARE_TIMEOUT);

        } else if (ar.status == LockStatus::FAIL) {
            ASSERT(ar.wound_rws.size() == 0);
            //Debug("[%lu] Participant prepare failed", transaction_id);
            LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);

            // TODO: Handle timeout
            shard_clients_[coordinator]->PrepareAbort(
                transaction_id, shard_idx_,
                std::bind(&Server::PrepareAbortCallback, this, transaction_id,
                          placeholders::_1, placeholders::_2),
                [](int, Timestamp) {}, PREPARE_TIMEOUT);

            // Reply to client
            SendRWCommmitParticipantReplyFail(*remote, client_id, client_req_id);
            delete remote;
            delete reply;
            pending_rw_commit_p_replies_.erase(search);

            NotifyPendingRWs(transaction_id, rr.notify_rws);

            transactions_.AbortPrepare(transaction_id);
        } else if (ar.status == LockStatus::WAITING) {
            Debug("[%lu] Waiting", transaction_id);

            transactions_.PausePrepare(transaction_id);

            WoundPendingRWs(transaction_id, ar.wound_rws);
        } else {
            NOT_REACHABLE();
        }
    } else if (s == PREPARED || s == COMMITTING || s == COMMITTED) {
        //Debug("[%lu] Already prepared", transaction_id);
    } else if (s == ABORTED) {  // Already aborted
        //Debug("[%lu] Already aborted", transaction_id);
    } else {
        NOT_REACHABLE();
    }
}

void Server::PrepareCallback(uint64_t transaction_id, int status, Timestamp timestamp) {
    TransactionState s = transactions_.FinishParticipantPrepare(transaction_id);
    if (s == PREPARED) {
        int coordinator = transactions_.GetCoordinator(transaction_id);
        const Timestamp &prepare_ts = transactions_.GetPrepareTimestamp(transaction_id);
        const Timestamp &nonblock_ts = transactions_.GetNonBlockTimestamp(transaction_id);
        // TODO: Handle timeout
        shard_clients_[coordinator]->PrepareOK(
            transaction_id, shard_idx_, prepare_ts, nonblock_ts,
            std::bind(&Server::PrepareOKCallback, this, transaction_id,
                      placeholders::_1, placeholders::_2),
            [](int, Timestamp) {}, PREPARE_TIMEOUT);

        // Reply to client
        SendRWCommmitParticipantReplyOK(transaction_id);

    } else if (s == ABORTED) {  // Already aborted

        SendRWCommmitParticipantReplyFail(transaction_id);

    } else {
        NOT_REACHABLE();
    }
}

void Server::PrepareOKCallback(uint64_t transaction_id, int status, Timestamp commit_ts) {
    //Debug("[%lu] Received PREPARE_OK callback: %d %d", transaction_id, shard_idx_, status);

    if (status == REPLY_OK) {
        TransactionState s = transactions_.ParticipantReceivePrepareOK(transaction_id);
        ASSERT(s == COMMITTING);

        // TODO: Handle timeout
        replica_client_->Commit(
            transaction_id, commit_ts,
            std::bind(&Server::CommitParticipantCallback, this, transaction_id, std::placeholders::_1),
            []() {}, COMMIT_TIMEOUT);

    } else if (status == REPLY_FAIL) {
        TransactionState s = transactions_.GetRWTransactionState(transaction_id);
        if (s == ABORTED) {
            //Debug("[%lu] Already aborted", transaction_id);
            return;
        }

        ASSERT(s == PREPARED);

        const Transaction &transaction = transactions_.GetTransaction(transaction_id);

        LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);
        TransactionFinishResult fr = transactions_.Abort(transaction_id);

        // TODO: Handle timeout
        replica_client_->Abort(
            transaction_id,
            std::bind(&Server::AbortParticipantCallback, this, transaction_id),
            []() {}, ABORT_TIMEOUT);

        NotifyPendingRWs(transaction_id, rr.notify_rws);
        NotifyPendingROs(fr.notify_ros);
        NotifySlowPathROs(fr.notify_slow_path_ros, transaction_id, false);

    } else {
        NOT_REACHABLE();
    }
}

void Server::PrepareAbortCallback(uint64_t transaction_id, int status,
                                  Timestamp timestamp) {
    ASSERT(status == REPLY_OK);

    //Debug("[%lu] Received PREPARE_ABORT callback: %d %d", transaction_id, shard_idx_, status);
}

void Server::CommitParticipantCallback(uint64_t transaction_id, transaction_status_t status) {
    ASSERT(status == REPLY_OK);

    //Debug("[%lu] Received COMMIT participant callback: %d %d", transaction_id, status, shard_idx_);
}

void Server::AbortParticipantCallback(uint64_t transaction_id) {
    //Debug("[%lu] Received ABORT participant callback: %d", transaction_id, shard_idx_);
}

void Server::HandlePrepareOK(const TransportAddress &remote, proto::PrepareOK &msg) {
    uint64_t client_id = msg.rid().client_id();
    uint64_t client_req_id = msg.rid().client_req_id();

    uint64_t transaction_id = msg.transaction_id();

    int participant_shard = msg.participant_shard();
    const Timestamp prepare_ts{msg.prepare_timestamp()};
    const Timestamp nonblock_ts{msg.nonblock_timestamp()};

    //Debug("[%lu] Received Prepare OK", transaction_id);

    PendingPrepareOKReply *reply = nullptr;
    auto search = pending_prepare_ok_replies_.find(transaction_id);
    if (search == pending_prepare_ok_replies_.end()) {
        reply = new PendingPrepareOKReply(client_id, client_req_id, remote.clone());
        pending_prepare_ok_replies_[transaction_id] = reply;
    } else {
        reply = pending_prepare_ok_replies_[transaction_id];
    }

    // Check for duplicates
    if (reply->rids.count({client_id, client_req_id, nullptr}) == 0) {
        reply->rids.insert({client_id, client_req_id, remote.clone()});
    }

    TransactionState s = transactions_.CoordinatorReceivePrepareOK(transaction_id, participant_shard, prepare_ts, nonblock_ts);
    if (s == PREPARING) {
        //Debug("[%lu] Coordinator preparing", transaction_id);

        const std::unordered_set<int> &participants = transactions_.GetParticipants(transaction_id);
        const Transaction &transaction = transactions_.GetTransaction(transaction_id);

        LockAcquireResult ar = locks_.AcquireLocks(transaction_id, transaction);
        if (ar.status == LockStatus::ACQUIRED) {
            ASSERT(ar.wound_rws.size() == 0);
            const Timestamp prepare_ts = GetPrepareTimestamp(client_id);
            transactions_.FinishCoordinatorPrepare(transaction_id, prepare_ts);

            const Timestamp &commit_ts = transactions_.GetRWCommitTimestamp(transaction_id);
            const Timestamp &start_ts = transactions_.GetStartTimestamp(transaction_id);
            const Timestamp &nonblock_ts = transactions_.GetNonBlockTimestamp(transaction_id);

            // TODO: Handle timeout
            replica_client_->CoordinatorCommit(
                transaction_id, start_ts, shard_idx_,
                participants, transaction, nonblock_ts, commit_ts,
                std::bind(&Server::CommitCoordinatorCallback, this,
                          transaction_id, std::placeholders::_1),
                []() {}, COMMIT_TIMEOUT);
        } else if (ar.status == FAIL) {
            ASSERT(ar.wound_rws.size() == 0);
            //Debug("[%lu] Coordinator prepare failed", transaction_id);
            LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);

            // Reply to participants
            SendPrepareOKRepliesFail(reply);
            delete reply;
            pending_prepare_ok_replies_.erase(transaction_id);

            // Notify other participants
            SendAbortParticipants(transaction_id, participants);

            // Reply to client
            PendingRWCommitCoordinatorReply *cr = pending_rw_commit_c_replies_[transaction_id];
            uint64_t client_id = cr->rid.client_id();
            uint64_t client_req_id = cr->rid.client_req_id();
            const TransportAddress *addr = cr->rid.addr();
            SendRWCommmitCoordinatorReplyFail(*addr, client_id, client_req_id);
            delete addr;
            delete cr;
            pending_rw_commit_c_replies_.erase(transaction_id);

            // Notify waiting RW transactions
            NotifyPendingRWs(transaction_id, rr.notify_rws);

            transactions_.AbortPrepare(transaction_id);
        } else if (ar.status == WAITING) {
            Debug("[%lu] Waiting", transaction_id);

            transactions_.PausePrepare(transaction_id);

            WoundPendingRWs(transaction_id, ar.wound_rws);
        } else {
            NOT_REACHABLE();
        }

    } else if (s == ABORTED) {  // Already aborted
        //Debug("[%lu] Already aborted", transaction_id);

        // Reply to participants
        SendPrepareOKRepliesFail(reply);
        delete reply;
        pending_prepare_ok_replies_.erase(transaction_id);

    } else if (s == WAIT_PARTICIPANTS) {
        //Debug("[%lu] Waiting for other participants", transaction_id);
    } else {
        NOT_REACHABLE();
    }
}

void Server::HandlePrepareAbort(const TransportAddress &remote, proto::PrepareAbort &msg) {
    uint64_t transaction_id = msg.transaction_id();

    //Debug("[%lu] Received Prepare ABORT", transaction_id);

    prepare_abort_reply_.mutable_rid()->CopyFrom(msg.rid());

    TransactionState state = transactions_.GetRWTransactionState(transaction_id);
    if (state == NOT_FOUND) {
        //Debug("[%lu] Transaction not in progress", transaction_id);

        prepare_abort_reply_.set_status(REPLY_OK);
        transport_->SendMessage(this, remote, prepare_abort_reply_);

        TransactionFinishResult fr = transactions_.Abort(transaction_id);
        ASSERT(fr.notify_ros.size() == 0);
        ASSERT(fr.notify_slow_path_ros.size() == 0);
        return;
    }

    if (state == ABORTED) {  // Already aborted
        //Debug("[%lu] Transaction already aborted", transaction_id);

        prepare_abort_reply_.set_status(REPLY_OK);
        transport_->SendMessage(this, remote, prepare_abort_reply_);
        return;
    }

    ASSERT(state == READING || state == READ_WAIT || state == WAIT_PARTICIPANTS);

    // Release locks acquired during GETs
    const Transaction &transaction = transactions_.GetTransaction(transaction_id);
    LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);

    // Reply to client
    auto search = pending_rw_commit_c_replies_.find(transaction_id);
    if (search != pending_rw_commit_c_replies_.end()) {
        PendingRWCommitCoordinatorReply *reply = search->second;

        uint64_t client_id = reply->rid.client_id();
        uint64_t client_req_id = reply->rid.client_req_id();
        const TransportAddress *addr = reply->rid.addr();

        SendRWCommmitCoordinatorReplyFail(*addr, client_id, client_req_id);

        std::unordered_set<int> participants = transactions_.GetParticipants(transaction_id);

        // Notify participants
        SendAbortParticipants(transaction_id, participants);

        delete addr;
        delete reply;
        pending_rw_commit_c_replies_.erase(search);
    }

    // Reply to OK participants
    auto search2 = pending_prepare_ok_replies_.find(transaction_id);
    if (search2 != pending_prepare_ok_replies_.end()) {
        PendingPrepareOKReply *reply = search2->second;
        SendPrepareOKRepliesFail(reply);
        delete reply;
        pending_prepare_ok_replies_.erase(search2);
    }

    prepare_abort_reply_.set_status(REPLY_OK);
    transport_->SendMessage(this, remote, prepare_abort_reply_);

    NotifyPendingRWs(transaction_id, rr.notify_rws);

    transactions_.Abort(transaction_id);
}

void Server::HandleWound(const TransportAddress &remote, proto::Wound &msg) {
    uint64_t transaction_id = msg.transaction_id();

    //Debug("[%lu] Received Wound request", transaction_id);

    TransactionState state = transactions_.GetRWTransactionState(transaction_id);

    if (state == ABORTED) {
        //Debug("[%lu] Transaction already aborted", transaction_id);
        return;
    }

    if (state == COMMITTING || state == COMMITTED) {
        //Debug("[%lu] Transaction already committing", transaction_id);
        return;
    }

    ASSERT(state != PREPARED);

    if (state == PREPARING || state == WAIT_PARTICIPANTS || state == PREPARE_WAIT) {
        // Only coordinator should handle wounds
        ASSERT(transactions_.GetCoordinator(transaction_id) == shard_idx_);

        // Reply to client
        auto search = pending_rw_commit_c_replies_.find(transaction_id);
        if (search != pending_rw_commit_c_replies_.end()) {
            PendingRWCommitCoordinatorReply *reply = search->second;

            uint64_t client_id = reply->rid.client_id();
            uint64_t client_req_id = reply->rid.client_req_id();
            const TransportAddress *addr = reply->rid.addr();

            SendRWCommmitCoordinatorReplyFail(*addr, client_id, client_req_id);

            delete addr;
            delete reply;
            pending_rw_commit_c_replies_.erase(search);
        }

        // Reply to OK participants
        auto search2 = pending_prepare_ok_replies_.find(transaction_id);
        if (search2 != pending_prepare_ok_replies_.end()) {
            PendingPrepareOKReply *reply = search2->second;
            SendPrepareOKRepliesFail(reply);
            delete reply;
            pending_prepare_ok_replies_.erase(search2);
        }

        const std::unordered_set<int> &participants = transactions_.GetParticipants(transaction_id);
        SendAbortParticipants(transaction_id, participants);
    }

    LockReleaseResult rr;
    TransactionFinishResult fr;

    // Coordinator may not yet know about this transaction
    // If so, no locks to release.
    if (state != NOT_FOUND) {
        const Transaction &transaction = transactions_.GetTransaction(transaction_id);
        rr = locks_.ReleaseLocks(transaction_id, transaction);
    }

    fr = transactions_.Abort(transaction_id);

    NotifyPendingRWs(transaction_id, rr.notify_rws);
    NotifyPendingROs(fr.notify_ros);
    NotifySlowPathROs(fr.notify_slow_path_ros, transaction_id, false);
}

void Server::HandleAbort(const TransportAddress &remote, proto::Abort &msg) {
    uint64_t transaction_id = msg.transaction_id();

    Debug("[%lu] Received Abort request", transaction_id);

    abort_reply_.mutable_rid()->CopyFrom(msg.rid());

    TransactionState state = transactions_.GetRWTransactionState(transaction_id);

    if (state == ABORTED) {
        //Debug("[%lu] Transaction already aborted", transaction_id);
        abort_reply_.set_status(REPLY_OK);
        transport_->SendMessage(this, remote, abort_reply_);
        return;
    }

    if (state == COMMITTING || state == COMMITTED) {
        //Debug("[%lu] Transaction already committing", transaction_id);
        abort_reply_.set_status(REPLY_FAIL);
        transport_->SendMessage(this, remote, abort_reply_);
        return;
    }

    LockReleaseResult rr;
    TransactionFinishResult fr;

    // Participant may not yet know about this transaction
    // If so, no locks to release.
    if (state != NOT_FOUND) {
        const Transaction &transaction = transactions_.GetTransaction(transaction_id);
        rr = locks_.ReleaseLocks(transaction_id, transaction);
    }

    fr = transactions_.Abort(transaction_id);

    if (state == PREPARING || state == PREPARED) {
        // TODO: Handle timeout
        replica_client_->Abort(
            transaction_id,
            std::bind(&Server::AbortParticipantCallback, this, transaction_id),
            []() {}, ABORT_TIMEOUT);
    }

    abort_reply_.set_status(REPLY_OK);
    transport_->SendMessage(this, remote, abort_reply_);

    // Reply to client for any ongoing GETs
    ContinueGet(transaction_id);

    NotifyPendingRWs(transaction_id, rr.notify_rws);
    NotifyPendingROs(fr.notify_ros);
    NotifySlowPathROs(fr.notify_slow_path_ros, transaction_id, false);
}

void Server::SendAbortParticipants(uint64_t transaction_id, const std::unordered_set<int> &participants) {
    for (int p : participants) {
        if (p != shard_idx_) {  // Don't send abort to self (coordinator)
            // TODO: Handle timeout
            shard_clients_[p]->Abort(
                transaction_id,
                [transaction_id]() { /*Debug("[%lu] Received ABORT participant callback", transaction_id);*/ },
                []() {}, ABORT_TIMEOUT);
        }
    }
}

void Server::CoordinatorCommitTransaction(uint64_t transaction_id, const Timestamp commit_ts) {
    //Debug("[%lu] Commiting", transaction_id);

    const Timestamp nonblock_ts = transactions_.GetNonBlockTimestamp(transaction_id);

    // Commit writes
    const Transaction &transaction = transactions_.GetTransaction(transaction_id);
    for (auto &write : transaction.getWriteSet()) {
        store_.put(write.first, write.second, {commit_ts, transaction_id});
    }

    if (transaction.getWriteSet().size() > 0) {
        min_prepare_timestamp_ = std::max(min_prepare_timestamp_, commit_ts);
    }

    LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);
    TransactionFinishResult fr = transactions_.Commit(transaction_id);

    // Reply to client
    SendRWCommmitCoordinatorReplyOK(transaction_id, commit_ts, nonblock_ts);

    // Reply to participants
    SendPrepareOKRepliesOK(transaction_id, commit_ts);

    // Continue waiting RW transactions
    NotifyPendingRWs(transaction_id, rr.notify_rws);

    // Continue waiting RO transactions
    NotifyPendingROs(fr.notify_ros);
    NotifySlowPathROs(fr.notify_slow_path_ros, transaction_id, true, commit_ts);
}

void Server::ParticipantCommitTransaction(uint64_t transaction_id, const Timestamp commit_ts) {
    //Debug("[%lu] Commiting", transaction_id);

    // Commit writes
    const Transaction &transaction = transactions_.GetTransaction(transaction_id);
    for (auto &write : transaction.getWriteSet()) {
        store_.put(write.first, write.second, {commit_ts, transaction_id});
    }

    if (transaction.getWriteSet().size() > 0) {
        min_prepare_timestamp_ = std::max(min_prepare_timestamp_, commit_ts);
    }

    LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);
    TransactionFinishResult fr = transactions_.Commit(transaction_id);

    // Continue waiting RW transactions
    NotifyPendingRWs(transaction_id, rr.notify_rws);

    // Continue waiting RO transactions
    NotifyPendingROs(fr.notify_ros);
    NotifySlowPathROs(fr.notify_slow_path_ros, transaction_id, true, commit_ts);
}

void Server::LeaderUpcall(opnum_t opnum, const string &op, bool &replicate,
                          string &response) {
    //Debug("Received LeaderUpcall: %lu %s", opnum, op.c_str());

    Request request;

    request.ParseFromString(op);

    switch (request.op()) {
        case strongstore::proto::Request::PREPARE:
        case strongstore::proto::Request::COMMIT:
        case strongstore::proto::Request::ABORT:
            replicate = true;
            response = op;
            break;
        default:
            Panic("Unrecognized operation.");
    }
}

/* Gets called when a command is issued using client.Invoke(...) to this
 * replica group.
 * opnum is the operation number.
 * op is the request string passed by the client.
 * response is the reply which will be sent back to the client.
 */
void Server::ReplicaUpcall(opnum_t opnum, const string &op, string &response) {
    //Debug("Received Upcall: %lu %s", opnum, op.c_str());
    Request request;
    Reply reply;

    request.ParseFromString(op);

    int status = REPLY_OK;
    uint64_t transaction_id = request.txnid();

    if (request.op() == strongstore::proto::Request::PREPARE) {
        //Debug("[%lu] Received PREPARE", transaction_id);

        TransactionState s = transactions_.GetRWTransactionState(transaction_id);
        if (s == ABORTED) {
            //Debug("[%lu] Already aborted", transaction_id);
            status = REPLY_FAIL;
        } else if (s == NOT_FOUND) {  // Replica prepare
            const Timestamp prepare_ts{request.prepare().timestamp()};
            int coordinator = request.prepare().coordinator();
            const Transaction transaction{request.prepare().txn()};
            const Timestamp nonblock_ts{request.prepare().nonblock_ts()};

            s = transactions_.StartParticipantPrepare(transaction_id, coordinator, transaction, nonblock_ts);
            ASSERT(s == PREPARING);

            LockAcquireResult ar = locks_.AcquireLocks(transaction_id, transaction);
            ASSERT(ar.status == LockStatus::ACQUIRED);

            transactions_.SetParticipantPrepareTimestamp(transaction_id, prepare_ts);

            transactions_.FinishParticipantPrepare(transaction_id);
        } else if (s == PREPARING || s == PREPARED) {
            //Debug("[%lu] Already prepared", transaction_id);
        } else {
            NOT_REACHABLE();
        }
    } else if (request.op() == strongstore::proto::Request::COMMIT) {
        //Debug("[%lu] Received COMMIT", transaction_id);

        const Timestamp commit_ts{request.commit().commit_timestamp()};

        if (request.has_prepare()) {  // Coordinator commit
            //Debug("[%lu] Coordinator commit", transaction_id);

            if (transactions_.GetRWTransactionState(transaction_id) != COMMITTING) {
                const Timestamp start_ts{request.prepare().timestamp()};
                int coordinator = request.prepare().coordinator();
                const std::unordered_set<int> participants{request.prepare().participants().begin(),
                                                           request.prepare().participants().end()};
                const Transaction transaction{request.prepare().txn()};
                const Timestamp nonblock_ts{request.prepare().nonblock_ts()};

                ASSERT(coordinator == shard_idx_);

                TransactionState s = transactions_.StartCoordinatorPrepare(transaction_id, start_ts, coordinator,
                                                                           participants, transaction, nonblock_ts);
                for (int p : participants) {
                    if (p != coordinator) {
                        s = transactions_.CoordinatorReceivePrepareOK(transaction_id, p, commit_ts, nonblock_ts);
                    }
                }
                ASSERT(s == PREPARING);

                LockAcquireResult ar = locks_.AcquireLocks(transaction_id, transaction);
                ASSERT(ar.status == LockStatus::ACQUIRED);

                transactions_.FinishCoordinatorPrepare(transaction_id, commit_ts);
            } else {
                //Debug("[%lu] Already prepared", transaction_id);
            }

            uint64_t commit_wait_us = tt_.TimeToWaitUntilMicros(commit_ts.getTimestamp());
            Debug("[%lu] delaying commit by %lu us", transaction_id, commit_wait_us);
            transport_->TimerMicro(commit_wait_us, std::bind(&Server::CoordinatorCommitTransaction, this, transaction_id, commit_ts));
        } else {  // Participant commit
            //Debug("[%lu] Participant commit", transaction_id);
            if (transactions_.GetRWTransactionState(transaction_id) != COMMITTING) {
                transactions_.ParticipantReceivePrepareOK(transaction_id);
            }

            ParticipantCommitTransaction(transaction_id, commit_ts);
        }

    } else if (request.op() == strongstore::proto::Request::ABORT) {
        //Debug("[%lu] Received ABORT", transaction_id);

        if (transactions_.GetRWTransactionState(transaction_id) != ABORTED) {  // replica abort
            const Transaction &transaction = transactions_.GetTransaction(transaction_id);

            LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);
            TransactionFinishResult fr = transactions_.Abort(transaction_id);

            NotifyPendingRWs(transaction_id, rr.notify_rws);
            NotifyPendingROs(fr.notify_ros);
            NotifySlowPathROs(fr.notify_slow_path_ros, transaction_id, false);
        }
    } else {
        NOT_REACHABLE();
    }

    reply.set_status(status);
    reply.SerializeToString(&response);
}

void Server::UnloggedUpcall(const string &op, string &response) {
    NOT_IMPLEMENTED();
}

void Server::Load(const string &key, const string &value,
                  const Timestamp timestamp) {
    store_.put(key, value, {timestamp, 0});
}

}  // namespace strongstore
