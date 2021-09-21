#include "store/strongstore/shardclient.h"

#include "lib/configuration.h"

namespace strongstore {

using namespace std;
using namespace proto;

ShardClient::ShardClient(const transport::Configuration &config,
                         Transport *transport, uint64_t client_id, int shard,
                         wound_callback wcb)
    : last_req_id_{0},
      config_{config},
      transport_{transport},
      client_id_{client_id},
      shard_idx_{shard},
      wcb_{wcb} {
    transport_->Register(this, config_, -1, -1);

    // TODO: Remove hardcoding
    replica_ = 0;
}

ShardClient::~ShardClient() {}

void ShardClient::ReceiveMessage(const TransportAddress &remote,
                                 const std::string &type,
                                 const std::string &data, void *meta_data) {
    if (type == get_reply_.GetTypeName()) {
        get_reply_.ParseFromString(data);
        HandleGetReply(get_reply_);
    } else if (type == rw_commit_c_reply_.GetTypeName()) {
        rw_commit_c_reply_.ParseFromString(data);
        HandleRWCommitCoordinatorReply(rw_commit_c_reply_);
    } else if (type == rw_commit_p_reply_.GetTypeName()) {
        rw_commit_p_reply_.ParseFromString(data);
        HandleRWCommitParticipantReply(rw_commit_p_reply_);
    } else if (type == prepare_ok_reply_.GetTypeName()) {
        prepare_ok_reply_.ParseFromString(data);
        HandlePrepareOKReply(prepare_ok_reply_);
    } else if (type == prepare_abort_reply_.GetTypeName()) {
        prepare_abort_reply_.ParseFromString(data);
        HandlePrepareAbortReply(prepare_abort_reply_);
    } else if (type == ro_commit_reply_.GetTypeName()) {
        ro_commit_reply_.ParseFromString(data);
        HandleROCommitReply(ro_commit_reply_);
    } else if (type == ro_commit_slow_reply_.GetTypeName()) {
        ro_commit_slow_reply_.ParseFromString(data);
        HandleROCommitSlowReply(ro_commit_slow_reply_);
    } else if (type == abort_reply_.GetTypeName()) {
        abort_reply_.ParseFromString(data);
        HandleAbortReply(abort_reply_);
    } else if (type == wound_.GetTypeName()) {
        wound_.ParseFromString(data);
        HandleWound(wound_);
    } else {
        Panic("Received unexpected message type: %s", type.c_str());
    }
}

void ShardClient::HandleWound(const proto::Wound &msg) {
    uint64_t transaction_id = msg.transaction_id();
    Debug("Received wound for tid: %lu", transaction_id);
    wcb_(transaction_id);
};

/* Sends BEGIN to a single shard indexed by i. */
void ShardClient::Begin(uint64_t transaction_id, const Timestamp &start_time) {
    Debug("[%lu] [shard %i] BEGIN", transaction_id, shard_idx_);

    auto search = transactions_.find(transaction_id);
    ASSERT(search == transactions_.end());

    auto &t = transactions_[transaction_id];

    t.set_start_time(start_time);
}

bool ShardClient::CheckPriorReadsAndWrites(uint64_t transaction_id, const std::string &key, get_callback gcb) {
    auto search = transactions_.find(transaction_id);
    if (search == transactions_.end()) {
        return false;
    }

    auto &txn = search->second;

    // Read your own writes, check the write set first.
    auto wsearch = txn.getWriteSet().find(key);
    if (wsearch != txn.getWriteSet().end()) {
        gcb(REPLY_OK, key, wsearch->second, Timestamp());
        return true;
    }

    // Consistent reads, check the read set.
    auto rssearch = read_sets_.find(transaction_id);
    if (rssearch != read_sets_.end()) {
        auto &read_set = rssearch->second;
        auto rsearch = read_set.find(key);
        if (rsearch != read_set.end()) {
            gcb(REPLY_OK, key, rsearch->second, Timestamp());
            return true;
        }
    }

    return false;
}

void ShardClient::Get(uint64_t transaction_id, const std::string &key,
                      get_callback gcb, get_timeout_callback gtcb,
                      uint32_t timeout) {
    Get(transaction_id, key, gcb, gtcb, timeout, false);
}

void ShardClient::GetForUpdate(uint64_t transaction_id, const std::string &key,
                               get_callback gcb, get_timeout_callback gtcb,
                               uint32_t timeout) {
    Get(transaction_id, key, gcb, gtcb, timeout, true);
}

void ShardClient::Get(uint64_t transaction_id, const std::string &key,
                      get_callback gcb, get_timeout_callback gtcb,
                      uint32_t timeout, bool for_update) {
    // Send the GET operation to appropriate shard.
    Debug("[shard %i] Sending GET [%s]", shard_idx_, key.c_str());

    uint64_t req_id = last_req_id_++;
    PendingGet *pendingGet = new PendingGet(transaction_id, req_id);
    pendingGets[req_id] = pendingGet;
    pendingGet->key = key;
    pendingGet->gcb = gcb;
    pendingGet->gtcb = gtcb;

    auto search = transactions_.find(transaction_id);
    ASSERT(search != transactions_.end());
    auto &t = search->second;
    auto &start_ts = t.start_time();

    // TODO: Setup timeout
    get_.Clear();
    get_.mutable_rid()->set_client_id(client_id_);
    get_.mutable_rid()->set_client_req_id(req_id);
    get_.set_transaction_id(transaction_id);
    start_ts.serialize(get_.mutable_timestamp());
    get_.set_key(key);
    get_.set_for_update(for_update);

    transport_->SendMessageToReplica(this, shard_idx_, replica_, get_);
}

void ShardClient::HandleGetReply(const proto::GetReply &reply) {
    uint64_t req_id = reply.rid().client_req_id();
    int status = reply.status();

    auto itr = pendingGets.find(req_id);
    if (itr == pendingGets.end()) {
        Debug("[%d][%lu] GetReply for stale request.", shard_idx_, req_id);
        return;  // stale request
    }

    PendingGet *req = itr->second;
    uint64_t transaction_id = req->transaction_id;
    get_callback gcb = req->gcb;
    std::string key = req->key;
    pendingGets.erase(itr);
    delete req;

    Debug("[%lu] [shard %i] Received GET reply: %s %d",
          transaction_id, shard_idx_, key.c_str(), status);

    std::string val;
    Timestamp ts;
    if (status == REPLY_OK) {
        val = reply.val();
        ts = Timestamp(reply.timestamp());
    }

    Debug("[%lu] Added %lu.%lu to read set.", transaction_id, ts.getTimestamp(), ts.getID());
    transactions_[transaction_id].addReadSet(key, ts);
    read_sets_[transaction_id][key] = val;

    gcb(status, key, val, ts);
}

void ShardClient::Put(uint64_t transaction_id, const std::string &key, const std::string &value,
                      put_callback pcb, put_timeout_callback ptcb,
                      uint32_t timeout) {
    auto search = transactions_.find(transaction_id);
    ASSERT(search != transactions_.end());

    auto &t = search->second;
    t.addWriteSet(key, value);

    pcb(REPLY_OK, key, value);
}

void ShardClient::ROCommit(uint64_t transaction_id,
                           const std::vector<std::string> &keys,
                           const Timestamp &commit_timestamp,
                           const Timestamp &min_read_timestamp,
                           ro_commit_callback ccb,
                           ro_commit_slow_callback cscb,
                           ro_commit_timeout_callback ctcb, uint32_t timeout) {
    Debug("[%lu] [shard %i] Sending ROCommit", transaction_id, shard_idx_);

    uint64_t req_id = last_req_id_++;
    PendingROCommit *pendingROCommit = new PendingROCommit(transaction_id, req_id);
    pendingROCommits[req_id] = pendingROCommit;
    pendingROCommit->ccb = ccb;
    pendingROCommit->cscb = cscb;
    pendingROCommit->ctcb = ctcb;
    pendingROCommit->n_slow_replies = 0;

    // TODO: Setup timeout
    ro_commit_.mutable_rid()->set_client_id(client_id_);
    ro_commit_.mutable_rid()->set_client_req_id(req_id);
    ro_commit_.set_transaction_id(transaction_id);
    commit_timestamp.serialize(ro_commit_.mutable_commit_timestamp());
    min_read_timestamp.serialize(ro_commit_.mutable_min_timestamp());

    ro_commit_.clear_keys();
    for (auto &k : keys) {
        ro_commit_.add_keys(k.c_str());
    }

    transport_->SendMessageToReplica(this, shard_idx_, replica_, ro_commit_);
}

void ShardClient::HandleROCommitSlowReply(const proto::ROCommitSlowReply &reply) {
    uint64_t req_id = reply.rid().client_req_id();

    auto itr = pendingROCommits.find(req_id);
    if (itr == pendingROCommits.end()) {
        Debug("[%d][%lu] ROCommitReply for stale request.", shard_idx_, req_id);
        return;  // stale request
    }

    PendingROCommit *req = itr->second;
    ASSERT(req->n_slow_replies > 0);

    ro_commit_slow_callback cscb = req->cscb;

    uint64_t transaction_id = reply.transaction_id();
    const Timestamp commit_ts{reply.commit_timestamp()};
    bool is_commit = reply.is_commit();

    req->n_slow_replies -= 1;
    if (req->n_slow_replies == 0) {
        pendingROCommits.erase(itr);
        delete req;
    }

    cscb(shard_idx_, transaction_id, commit_ts, is_commit);
}

void ShardClient::HandleROCommitReply(const proto::ROCommitReply &reply) {
    uint64_t req_id = reply.rid().client_req_id();

    auto itr = pendingROCommits.find(req_id);
    if (itr == pendingROCommits.end()) {
        Debug("[%d][%lu] ROCommitReply for stale request.", shard_idx_, req_id);
        return;  // stale request
    }

    PendingROCommit *req = itr->second;
    ro_commit_callback ccb = req->ccb;
    ASSERT(req->n_slow_replies == 0);

    std::vector<Value> values;
    for (auto &v : reply.values()) {
        values.emplace_back(v);
    }

    std::vector<PreparedTransaction> prepares;
    for (auto &p : reply.prepares()) {
        prepares.emplace_back(p);
    }

    uint64_t n_prepares = prepares.size();
    if (n_prepares == 0) {
        pendingROCommits.erase(itr);
        delete req;
    } else {
        req->n_slow_replies = n_prepares;
    }

    ccb(shard_idx_, values, prepares);
}

void ShardClient::RWCommitCoordinator(
    uint64_t transaction_id,
    const std::set<int> participants, Timestamp &nonblock_timestamp,
    rw_coord_commit_callback ccb, rw_coord_commit_timeout_callback ctcb, uint32_t timeout) {
    Debug("[%lu] [shard %i] Sending RWCommitCoordinator", transaction_id, shard_idx_);

    auto search = transactions_.find(transaction_id);
    ASSERT(search != transactions_.end());

    const auto &t = search->second;

    uint64_t req_id = last_req_id_++;
    PendingRWCoordCommit *pendingCommit = new PendingRWCoordCommit(transaction_id, req_id);
    pendingRWCoordCommits[req_id] = pendingCommit;
    pendingCommit->ccb = ccb;
    pendingCommit->ctcb = ctcb;

    // TODO: Setup timeout
    rw_commit_c_.Clear();
    rw_commit_c_.mutable_rid()->set_client_id(client_id_);
    rw_commit_c_.mutable_rid()->set_client_req_id(req_id);
    rw_commit_c_.set_transaction_id(transaction_id);
    t.serialize(rw_commit_c_.mutable_transaction());
    nonblock_timestamp.serialize((rw_commit_c_.mutable_nonblock_timestamp()));

    for (int p : participants) {
        rw_commit_c_.add_participants(p);
    }

    transport_->SendMessageToReplica(this, shard_idx_, replica_, rw_commit_c_);
}

void ShardClient::HandleRWCommitCoordinatorReply(const proto::RWCommitCoordinatorReply &reply) {
    uint64_t req_id = reply.rid().client_req_id();

    auto itr = pendingRWCoordCommits.find(req_id);
    if (itr == pendingRWCoordCommits.end()) {
        Debug("[%d][%lu] RWCommitCoordinatorReply for stale request.", shard_idx_, req_id);
        return;  // stale request
    }

    PendingRWCoordCommit *req = itr->second;
    uint64_t transaction_id = req->transaction_id;
    rw_coord_commit_callback ccb = req->ccb;
    pendingRWCoordCommits.erase(itr);
    delete req;

    transactions_.erase(transaction_id);
    read_sets_.erase(transaction_id);

    Debug("[shard %i] COMMIT timestamp %lu.%lu", shard_idx_,
          reply.commit_timestamp().timestamp(), reply.commit_timestamp().id());
    ccb(reply.status(), Timestamp(reply.commit_timestamp()), Timestamp(reply.nonblock_timestamp()));
}

void ShardClient::RWCommitParticipant(uint64_t transaction_id,
                                      int coordinator_shard, Timestamp &nonblock_timestamp,
                                      rw_part_commit_callback ccb, rw_part_commit_timeout_callback ctcb,
                                      uint32_t timeout) {
    Debug("[%lu] [shard %i] Sending RWCommitParticipant", transaction_id, shard_idx_);

    auto search = transactions_.find(transaction_id);
    ASSERT(search != transactions_.end());

    const auto &t = search->second;

    uint64_t req_id = last_req_id_++;
    PendingRWParticipantCommit *pendingCommit = new PendingRWParticipantCommit(transaction_id, req_id);
    pendingRWParticipantCommits[req_id] = pendingCommit;
    pendingCommit->ccb = ccb;
    pendingCommit->ctcb = ctcb;

    // TODO: Setup timeout
    rw_commit_p_.Clear();
    rw_commit_p_.mutable_rid()->set_client_id(client_id_);
    rw_commit_p_.mutable_rid()->set_client_req_id(req_id);
    rw_commit_p_.set_transaction_id(transaction_id);
    t.serialize(rw_commit_p_.mutable_transaction());
    rw_commit_p_.set_coordinator_shard(coordinator_shard);
    nonblock_timestamp.serialize((rw_commit_p_.mutable_nonblock_timestamp()));

    transport_->SendMessageToReplica(this, shard_idx_, replica_, rw_commit_p_);
}

void ShardClient::HandleRWCommitParticipantReply(const proto::RWCommitParticipantReply &reply) {
    Debug("[shard %i] Received RWCommitParticipant", shard_idx_);
    uint64_t req_id = reply.rid().client_req_id();

    auto itr = pendingRWParticipantCommits.find(req_id);
    if (itr == pendingRWParticipantCommits.end()) {
        Debug("[%d][%lu] RWCommitParticipantReply for stale request.", shard_idx_, req_id);
        return;  // stale request
    }

    PendingRWParticipantCommit *req = itr->second;
    uint64_t transaction_id = req->transaction_id;
    rw_part_commit_callback ccb = req->ccb;
    pendingRWParticipantCommits.erase(itr);
    delete req;

    transactions_.erase(transaction_id);
    read_sets_.erase(transaction_id);

    ccb(reply.status());
}

void ShardClient::PrepareOK(uint64_t transaction_id, int participant_shard,
                            const Timestamp &prepare_timestamp, const Timestamp &nonblock_ts,
                            prepare_callback pcb,
                            prepare_timeout_callback ptcb, uint32_t timeout) {
    Debug("[shard %i] Sending PrepareOK [%lu]", shard_idx_, transaction_id);

    uint64_t req_id = last_req_id_++;
    PendingPrepareOK *pendingPrepareOK = new PendingPrepareOK(transaction_id, req_id);
    pendingPrepareOKs[req_id] = pendingPrepareOK;
    pendingPrepareOK->pcb = pcb;
    pendingPrepareOK->ptcb = ptcb;

    // TODO: Setup timeout
    prepare_ok_.mutable_rid()->set_client_id(client_id_);
    prepare_ok_.mutable_rid()->set_client_req_id(req_id);
    prepare_ok_.set_transaction_id(transaction_id);
    prepare_ok_.set_participant_shard(participant_shard);
    prepare_timestamp.serialize(prepare_ok_.mutable_prepare_timestamp());
    nonblock_ts.serialize(prepare_ok_.mutable_nonblock_timestamp());

    transport_->SendMessageToReplica(this, shard_idx_, replica_, prepare_ok_);
}

void ShardClient::HandlePrepareOKReply(const proto::PrepareOKReply &reply) {
    Debug("[shard %i] Received PrepareOKReply", shard_idx_);
    uint64_t req_id = reply.rid().client_req_id();

    auto itr = pendingPrepareOKs.find(req_id);
    if (itr == pendingPrepareOKs.end()) {
        Debug("[%d][%lu] PrepareOKReply for stale request.", shard_idx_,
              req_id);
        return;  // stale request
    }

    PendingPrepareOK *req = itr->second;
    prepare_callback pcb = req->pcb;
    pendingPrepareOKs.erase(itr);
    delete req;

    Debug("[shard %i] COMMIT timestamp [%lu.%lu]", shard_idx_,
          reply.commit_timestamp().timestamp(), reply.commit_timestamp().id());
    pcb(reply.status(), Timestamp(reply.commit_timestamp()));
}

void ShardClient::PrepareAbort(uint64_t transaction_id, int participant_shard,
                               prepare_callback pcb,
                               prepare_timeout_callback ptcb,
                               uint32_t timeout) {
    Debug("[shard %i] Sending PrepareAbort [%lu]", shard_idx_, transaction_id);

    uint64_t req_id = last_req_id_++;
    PendingPrepareAbort *pendingPrepareAbort = new PendingPrepareAbort(transaction_id, req_id);
    pendingPrepareAborts[req_id] = pendingPrepareAbort;
    pendingPrepareAbort->pcb = pcb;
    pendingPrepareAbort->ptcb = ptcb;

    // TODO: Setup timeout
    prepare_abort_.mutable_rid()->set_client_id(client_id_);
    prepare_abort_.mutable_rid()->set_client_req_id(req_id);
    prepare_abort_.set_transaction_id(transaction_id);
    prepare_abort_.set_participant_shard(participant_shard);

    transport_->SendMessageToReplica(this, shard_idx_, replica_,
                                     prepare_abort_);
}

void ShardClient::HandlePrepareAbortReply(
    const proto::PrepareAbortReply &reply) {
    Debug("[shard %i] Received PrepareAbortReply", shard_idx_);
    uint64_t req_id = reply.rid().client_req_id();

    auto itr = pendingPrepareAborts.find(req_id);
    if (itr == pendingPrepareAborts.end()) {
        Debug("[%d][%lu] PrepareAbortReply for stale request.", shard_idx_,
              req_id);
        return;  // stale request
    }

    PendingPrepareAbort *req = itr->second;
    prepare_callback pcb = req->pcb;
    pendingPrepareAborts.erase(itr);
    delete req;

    pcb(reply.status(), Timestamp());
}

void ShardClient::Abort(uint64_t transaction_id, abort_callback acb,
                        abort_timeout_callback atcb, uint32_t timeout) {
    Debug("[%lu] [shard %i] Sending Abort", transaction_id, shard_idx_);

    uint64_t req_id = last_req_id_++;
    PendingAbort *pendingAbort = new PendingAbort(transaction_id, req_id);
    pendingAborts[req_id] = pendingAbort;
    pendingAbort->acb = acb;
    pendingAbort->atcb = atcb;

    // TODO: Setup timeout
    abort_.Clear();
    abort_.mutable_rid()->set_client_id(client_id_);
    abort_.mutable_rid()->set_client_req_id(req_id);
    abort_.set_transaction_id(transaction_id);

    transport_->SendMessageToReplica(this, shard_idx_, replica_, abort_);
}

void ShardClient::Wound(uint64_t transaction_id) {
    Debug("[%lu] [shard %i] Sending wound", transaction_id, shard_idx_);

    wound_.set_transaction_id(transaction_id);

    transport_->SendMessageToReplica(this, shard_idx_, replica_, wound_);
}

void ShardClient::AbortGet(uint64_t transaction_id) {
    Debug("[%lu] [shard %i] Aborting GET", transaction_id, shard_idx_);

    for (auto it = pendingGets.begin(); it != pendingGets.end(); ++it) {
        if (it->second->transaction_id == transaction_id) {
            PendingGet *req = it->second;
            uint64_t transaction_id = req->transaction_id;
            get_callback gcb = req->gcb;
            std::string key = req->key;

            pendingGets.erase(it);
            delete req;

            gcb(REPLY_FAIL, key, "", {});
            break;
        }
    }
}

void ShardClient::AbortPut(uint64_t transaction_id) {
    Debug("[%lu] [shard %i] Aborting PUT", transaction_id, shard_idx_);

    Panic("No PUT in progress!");
}

void ShardClient::HandleAbortReply(const proto::AbortReply &reply) {
    Debug("[shard %i] Received HandleAbortReply", shard_idx_);
    uint64_t req_id = reply.rid().client_req_id();

    auto itr = pendingAborts.find(req_id);
    if (itr == pendingAborts.end()) {
        Debug("[%d][%lu] PrepareAbortReply for stale request.", shard_idx_,
              req_id);
        return;  // stale request
    }

    PendingAbort *req = itr->second;
    uint64_t transaction_id = req->transaction_id;
    abort_callback acb = req->acb;
    pendingAborts.erase(itr);
    delete req;

    if (reply.status() == REPLY_OK) {
        transactions_.erase(transaction_id);
        read_sets_.erase(transaction_id);
    }

    acb();
}

}  // namespace strongstore
