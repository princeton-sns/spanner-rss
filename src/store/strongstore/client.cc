#include "store/strongstore/client.h"

#include <rss/lib.h>

#include <cmath>
#include <cstdlib>
#include <functional>

#include "lib/configuration.h"
#include "lib/latency.h"
#include "store/common/common.h"

using namespace std;

namespace strongstore {

Client::Client(Consistency consistency, const NetworkConfiguration &net_config,
               const std::string &client_region,
               transport::Configuration &config, uint64_t client_id,
               int nShards, int closestReplica, Transport *transport,
               Partitioner *part, TrueTime &tt, bool debug_stats,
               double nb_time_alpha)
    : coord_choices_{},
      min_lats_{},
      sessions_{},
      sessions_by_transaction_id_{},
      net_config_{net_config},
      client_region_{client_region},
      service_name_{"spanner-" + std::to_string(client_id) + "-" + std::to_string(std::rand())},
      config_{config},
      client_id_{client_id},
      nshards_(nShards),
      transport_{transport},
      part_(part),
      tt_{tt},
      next_transaction_id_{client_id_ << 26},
      consistency_{consistency},
      nb_time_alpha_{nb_time_alpha},
      debug_stats_{debug_stats} {
    Debug("Initializing StrongStore client with id [%lu]", client_id_);

    auto wcb = std::bind(&Client::HandleWound, this, std::placeholders::_1);

    /* Start a client for each shard. */
    for (uint64_t i = 0; i < nshards_; i++) {
        ShardClient *shardclient = new ShardClient(config_, transport_, client_id_, i, wcb);
        sclients_.push_back(shardclient);
    }

    Debug("SpanStore client [%lu] created!", client_id_);

    if (debug_stats_) {
        _Latency_Init(&op_lat_, "op_lat");
        _Latency_Init(&commit_lat_, "commit_lat");
    }

    CalculateCoordinatorChoices();

    rss::RegisterRSSService(service_name_, std::bind(&Client::RealTimeBarrier, this, std::placeholders::_1, std::placeholders::_2));
}

Client::~Client() {
    rss::UnregisterRSSService(service_name_);

    if (debug_stats_) {
        Latency_Dump(&op_lat_);
        Latency_Dump(&commit_lat_);
    }

    for (auto s : sclients_) {
        delete s;
    }

    Debug("sessions_.size(): %lu", sessions_.size());
    Debug("sessions_by_transaction_id_.size(): %lu", sessions_by_transaction_id_.size());
}

void Client::CalculateCoordinatorChoices() {
    if (static_cast<std::size_t>(config_.g) > MAX_SHARDS) {
        Panic("CalculateCoordinatorChoices doesn't support more than %lu shards.", MAX_SHARDS);
    }

    std::vector<uint16_t> prepare_lats{};
    prepare_lats.reserve(config_.g);
    std::vector<uint16_t> commit_lats{};
    commit_lats.reserve(config_.g);

    for (int i = 0; i < config_.g; i++) {
        const std::string &leader_region = net_config_.GetRegion(i, 0);
        uint16_t min_q_lat = net_config_.GetMinQuorumLatency(i, 0);

        // Calculate prepare lat (including client to participant)
        uint16_t prepare_lat = net_config_.GetOneWayLatency(client_region_, leader_region);
        prepare_lat += min_q_lat;
        prepare_lats[i] = prepare_lat;

        // Calculate commit lat (including coordinator to client)
        uint16_t commit_lat = min_q_lat;
        commit_lat += net_config_.GetOneWayLatency(leader_region, client_region_);
        commit_lats[i] = commit_lat;
    }

    uint16_t s_max = (1 << config_.g) - 1;

    for (uint16_t s = 1; s != 0 && s <= s_max; s++) {
        std::bitset<MAX_SHARDS> shards{s};

        uint16_t min_lat = static_cast<uint16_t>(-1);
        int min_coord = -1;
        for (std::size_t coord_idx = 1; coord_idx <= shards.count(); coord_idx++) {
            // Find coord
            std::size_t coord = -1;
            std::size_t n_test = 0;
            for (std::size_t i = 0; i < MAX_SHARDS; i++) {
                if (shards.test(i)) {
                    n_test++;
                }

                if (n_test == coord_idx) {
                    coord = i;
                    break;
                }
            }

            const std::string &c_leader_region = net_config_.GetRegion(coord, 0);

            // Find max prepare lat
            uint16_t lat = 0;
            for (std::size_t i = 0; i < MAX_SHARDS; i++) {
                uint16_t l = 0;
                if (i == coord) {
                    l = net_config_.GetOneWayLatency(client_region_, c_leader_region);
                } else if (shards.test(i)) {
                    const std::string &p_leader_region = net_config_.GetRegion(i, 0);
                    l = prepare_lats[i] + net_config_.GetOneWayLatency(p_leader_region, c_leader_region);
                }

                lat = std::max(lat, l);
            }

            lat += commit_lats[coord];

            if (lat < min_lat) {
                min_lat = lat;
                min_coord = static_cast<int>(coord);
            }
        }

        // Not in wide area, pick random coordinator
        if (min_lat == 0) {
            std::size_t coord_idx = (client_id_ % shards.count()) + 1;

            // Find coord
            std::size_t n_test = 0;
            for (std::size_t i = 0; i < MAX_SHARDS; i++) {
                if (shards.test(i)) {
                    n_test++;
                }

                if (n_test == coord_idx) {
                    min_coord = i;
                    break;
                }
            }
            Debug("Choosing random coord: %d", min_coord);
        }

        coord_choices_.emplace(shards, min_coord);
        min_lats_.emplace(shards, min_lat);
    }

    Debug("Printing coord_choices:");
    for (auto &c : coord_choices_) {
        Debug("shards: %s, min_coord: %d", c.first.to_string().c_str(),
              c.second);
    }

    Debug("Printing min_lats:");
    for (auto &c : min_lats_) {
        Debug("shards: %s, min_lat: %u", c.first.to_string().c_str(), c.second);
    }
}

int Client::ChooseCoordinator(StrongSession &session) {
    auto &participants = session.participants();
    ASSERT(participants.size() != 0);

    std::bitset<MAX_SHARDS> shards;

    for (int p : participants) {
        shards.set(p);
    }

    ASSERT(coord_choices_.find(shards) != coord_choices_.end());

    return coord_choices_[shards];
}

Timestamp Client::ChooseNonBlockTimestamp(StrongSession &session) {
    auto &participants = session.participants();
    ASSERT(participants.size() != 0);

    std::bitset<MAX_SHARDS> shards;

    for (int p : participants) {
        shards.set(p);
    }

    ASSERT(min_lats_.find(shards) != min_lats_.end());

    uint16_t l = min_lats_[shards];
    uint64_t lat = static_cast<uint64_t>(nb_time_alpha_ * l * 1000);
    auto now = tt_.Now();
    Debug("[%lu] lat: %lu, ts: %lu", session.id(), lat, now.earliest() + lat);
    return {now.earliest() + lat, client_id_};
}

void Client::HandleWound(const uint64_t transaction_id) {
    Debug("[%lu] Handling wound", transaction_id);

    auto search = sessions_by_transaction_id_.find(transaction_id);
    if (search == sessions_by_transaction_id_.end()) {
        Debug("[%lu] Transaction already finished", transaction_id);
        return;
    }
    auto &session = search->second;

    if (session.participants().size() == 0) {
        Debug("[%lu] RO Transaction...letting finish", transaction_id);
        return;
    }

    int p = -1;
    int coordinator = -1;
    Debug("[%lu] client state: %d", transaction_id, session.state());
    switch (session.state()) {
        case StrongSession::EXECUTING:
            session.set_needs_abort();
            break;
        case StrongSession::GETTING:
            p = session.current_participant();
            sclients_[p]->AbortGet(transaction_id);
            break;
        case StrongSession::PUTTING:
            p = session.current_participant();
            sclients_[p]->AbortPut(transaction_id);
            break;
        case StrongSession::COMMITTING:
            Debug("[%lu] Forwarding wound to coordinator", transaction_id);
            coordinator = ChooseCoordinator(session);
            sclients_[coordinator]->Wound(transaction_id);
            break;
        case StrongSession::ABORTING:
            Debug("[%lu] Already aborted", transaction_id);
            break;
        default:
            Panic("Unexpected state: %d", session.state());
    }
}

// Force transaction to abort.
void Client::ForceAbort(const uint64_t transaction_id) {
    HandleWound(transaction_id);
}

void Client::RealTimeBarrier(const rss::Session &session, rss::continuation_func_t continuation) {
    const uint64_t L = 10 * 1e3;  // TODO: Remove hardcoding

    Debug("[%lu] invoked real-time barrier at client %s!", session.id(), service_name_.c_str());

    auto search = tmins_.find(session.id());
    ASSERT(search != tmins_.end());

    auto &tmin = search->second;

    uint64_t wait_us = tt_.TimeToWaitUntilMicros(tmin.getTimestamp() + L);

    Debug("Waiting %lu us", wait_us);

    transport_->TimerMicro(wait_us, continuation);

    tmins_.erase(search);
}

Session &Client::BeginSession() {
    StrongSession session{};
    auto sid = session.id();

    sessions_.emplace(sid, std::move(session));

    return sessions_.find(sid)->second;
}

Session &Client::ContinueSession(rss::Session &rss_session) {
    auto sid = rss_session.id();

    sessions_.emplace(sid, StrongSession{std::move(rss_session)});

    return sessions_.find(sid)->second;
}

rss::Session Client::EndSession(Session &s) {
    auto &session = static_cast<StrongSession &>(s);

    auto sid = s.id();
    auto search = sessions_.find(sid);
    ASSERT(search != sessions_.end());

    tmins_.emplace(sid, session.min_read_ts());

    rss::Session rss_session = std::move(session);

    sessions_.erase(search);

    return std::move(rss_session);
}

/* Begins a transaction. All subsequent operations before a commit() or
 * abort() are part of this transaction.
 */
void Client::Begin(Session &session, begin_callback bcb, begin_timeout_callback btcb, uint32_t timeout) {
    rss::StartTransaction(service_name_, session, std::bind(&Client::ContinueBegin, this, std::ref(session), bcb));
}

void Client::ContinueBegin(Session &s, begin_callback bcb) {
    auto &session = static_cast<StrongSession &>(s);

    if (session.transaction_id() != static_cast<uint64_t>(-1)) {
        sessions_by_transaction_id_.erase(session.transaction_id());
    }

    auto tid = next_transaction_id_++;

    Debug("[%lu] Begin", tid);

    Timestamp start_ts{tt_.Now().latest(), client_id_};

    session.start_transaction(tid, start_ts);
    sessions_by_transaction_id_.emplace(tid, session);

    for (uint64_t i = 0; i < nshards_; i++) {
        sclients_[i]->Begin(tid, start_ts);
    }

    bcb();
}

/* Begins a transaction, retrying the transaction indicated by session.
 */
void Client::Retry(Session &session, begin_callback bcb, begin_timeout_callback btcb, uint32_t timeout) {
    rss::StartTransaction(service_name_, session, std::bind(&Client::ContinueRetry, this, std::ref(session), bcb));
}

void Client::ContinueRetry(Session &s, begin_callback bcb) {
    auto &session = static_cast<StrongSession &>(s);

    if (session.transaction_id() != static_cast<uint64_t>(-1)) {
        sessions_by_transaction_id_.erase(session.transaction_id());
    }

    auto tid = next_transaction_id_++;

    session.retry_transaction(tid);
    sessions_by_transaction_id_.emplace(tid, session);

    auto &start_ts = session.start_ts();
    for (uint64_t i = 0; i < nshards_; i++) {
        sclients_[i]->Begin(tid, start_ts);
    }

    bcb();
}

/* Returns the value corresponding to the supplied key. */
void Client::Get(Session &s, const std::string &key, get_callback gcb,
                 get_timeout_callback gtcb, uint32_t timeout) {
    auto &session = static_cast<StrongSession &>(s);

    auto tid = session.transaction_id();

    Debug("GET [%lu : %s]", tid, key.c_str());

    if (session.needs_aborts()) {
        Debug("[%lu] Need to abort", tid);
        gcb(REPLY_FAIL, "", "", Timestamp());
        return;
    }

    ASSERT(session.executing());

    // Contact the appropriate shard to get the value.
    int i = (*part_)(key, nshards_, -1, session.participants());

    session.set_getting(i);

    // Add this shard to set of participants
    session.add_participant(i);

    auto gcb1 = [gcb, session = std::ref(session)](int s, const std::string &k, const std::string &v, Timestamp ts) {
        session.get().set_executing();
        gcb(s, k, v, ts);
    };

    auto gtcb1 = [gtcb, session = std::ref(session)](int s, const std::string &k) {
        session.get().set_executing();
        gtcb(s, k);
    };

    // Send the GET operation to appropriate shard.
    sclients_[i]->Get(tid, key, gcb1, gtcb1, timeout);
}

/* Returns the value corresponding to the supplied key. */
void Client::GetForUpdate(Session &s, const std::string &key, get_callback gcb,
                          get_timeout_callback gtcb, uint32_t timeout) {
    auto &session = static_cast<StrongSession &>(s);

    auto tid = session.transaction_id();

    Debug("GET FOR UPDATE [%lu : %s]", tid, key.c_str());

    if (session.needs_aborts()) {
        Debug("[%lu] Need to abort", tid);
        gcb(REPLY_FAIL, "", "", Timestamp());
        return;
    }

    ASSERT(session.executing());

    // Contact the appropriate shard to get the value.
    int i = (*part_)(key, nshards_, -1, session.participants());

    session.set_getting(i);

    // Add this shard to set of participants
    session.add_participant(i);

    auto gcb1 = [gcb, session = std::ref(session)](int s, const std::string &k, const std::string &v, Timestamp ts) {
        session.get().set_executing();
        gcb(s, k, v, ts);
    };

    auto gtcb1 = [gtcb, session = std::ref(session)](int s, const std::string &k) {
        session.get().set_executing();
        gtcb(s, k);
    };

    // Send the GET operation to appropriate shard.
    sclients_[i]->GetForUpdate(tid, key, gcb1, gtcb1, timeout);
}

/* Sets the value corresponding to the supplied key. */
void Client::Put(Session &s, const std::string &key, const std::string &value,
                 put_callback pcb, put_timeout_callback ptcb, uint32_t timeout) {
    auto &session = static_cast<StrongSession &>(s);

    auto tid = session.transaction_id();

    Debug("PUT [%lu : %s]", tid, key.c_str());

    if (session.needs_aborts()) {
        Debug("[%lu] Need to abort", tid);
        pcb(REPLY_FAIL, "", "");
        return;
    }

    ASSERT(session.executing());

    // Contact the appropriate shard to set the value.
    int i = (*part_)(key, nshards_, -1, session.participants());

    session.set_putting(i);

    // Add this shard to set of participants
    session.add_participant(i);

    auto pcb1 = [pcb, session = std::ref(session)](int s, const std::string &k, const std::string &v) {
        session.get().set_executing();
        pcb(s, k, v);
    };

    auto ptcb1 = [ptcb, session = std::ref(session)](int s, const std::string &k, const std::string &v) {
        session.get().set_executing();
        ptcb(s, k, v);
    };

    sclients_[i]->Put(tid, key, value, pcb1, ptcb1, timeout);
}

/* Attempts to commit the ongoing transaction. */
void Client::Commit(Session &s, commit_callback ccb, commit_timeout_callback ctcb, uint32_t timeout) {
    auto &session = static_cast<StrongSession &>(s);

    auto tid = session.transaction_id();

    Debug("[%lu] COMMIT", tid);

    if (session.needs_aborts()) {
        Debug("[%lu] Need to abort", tid);
        ccb(ABORTED_SYSTEM);
        return;
    }

    ASSERT(session.executing());
    session.set_committing();

    auto &min_read_ts = session.min_read_ts();
    Debug("[%lu] min_read_ts: %lu.%lu", tid, min_read_ts.getTimestamp(), min_read_ts.getID());

    uint64_t req_id = last_req_id_++;
    PendingRequest *req = new PendingRequest(req_id);
    pending_reqs_[req_id] = req;
    req->ccb = ccb;
    req->ctcb = ctcb;

    auto &participants = session.participants();

    stats.IncrementList("txn_groups", participants.size());

    Debug("[%lu] PREPARE", tid);
    ASSERT(participants.size() > 0);

    req->outstandingPrepares = 0;

    int coordinator_shard = ChooseCoordinator(session);

    Timestamp nonblock_timestamp = Timestamp();
    if (consistency_ == Consistency::RSS) {
        nonblock_timestamp = ChooseNonBlockTimestamp(session);
    }

    auto cccb = std::bind(&Client::CommitCallback, this, std::ref(session), req->id,
                          std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
    auto cctcb = [](int) {};

    auto pccb = [transaction_id = tid](int status) {
        Debug("[%lu] PREPARE callback status %d", transaction_id, status);
    };
    auto pctcb = [](int) {};

    for (auto p : participants) {
        if (p == coordinator_shard) {
            sclients_[p]->RWCommitCoordinator(tid, participants, nonblock_timestamp, cccb, cctcb, timeout);
        } else {
            sclients_[p]->RWCommitParticipant(tid, coordinator_shard, nonblock_timestamp, pccb, pctcb, timeout);
        }
    }
}

void Client::CommitCallback(StrongSession &session, uint64_t req_id, int status, Timestamp commit_ts, Timestamp nonblock_ts) {
    auto tid = session.transaction_id();
    Debug("[%lu] COMMIT callback status %d", tid, status);

    auto search = pending_reqs_.find(req_id);
    if (search == pending_reqs_.end()) {
        Debug("[%lu] Transaction already finished", tid);
        return;
    }
    PendingRequest *req = search->second;

    transaction_status_t tstatus;
    switch (status) {
        case REPLY_OK:
            Debug("[%lu] COMMIT OK", tid);
            tstatus = COMMITTED;
            break;
        default:
            // abort!
            Debug("[%lu] COMMIT ABORT", tid);
            tstatus = ABORTED_SYSTEM;
            break;
    }

    commit_callback ccb = req->ccb;
    pending_reqs_.erase(req_id);
    delete req;

    uint64_t ms = 0;
    if (tstatus == COMMITTED && consistency_ == Consistency::RSS) {
        ms = tt_.TimeToWaitUntilMS(nonblock_ts.getTimestamp());
        Debug("Waiting for nonblock time: %lu ms", ms);
        session.advance_min_read_ts(commit_ts);
        auto &min_read_ts = session.min_read_ts();
        Debug("min_read_timestamp_: %lu.%lu", min_read_ts.getTimestamp(), min_read_ts.getID());
    }

    rss::EndTransaction(service_name_, session);

    transport_->Timer(ms, std::bind(ccb, tstatus));
}

void Client::Abort(Session &s, abort_callback acb, abort_timeout_callback atcb, uint32_t timeout) {
    auto &session = static_cast<StrongSession &>(s);

    auto tid = session.transaction_id();
    Debug("[%lu] ABORT", tid);

    ASSERT(session.needs_aborts() || session.executing());
    session.set_aborting();

    auto &participants = session.participants();

    uint64_t req_id = last_req_id_++;
    PendingRequest *req = new PendingRequest(req_id);
    pending_reqs_[req_id] = req;
    req->acb = acb;
    req->atcb = atcb;
    req->outstandingPrepares = participants.size();

    auto cb = std::bind(&Client::AbortCallback, this, std::ref(session), req->id);
    auto tcb = []() {};

    for (int p : participants) {
        sclients_[p]->Abort(tid, cb, tcb, timeout);
    }
}

void Client::AbortCallback(StrongSession &session, uint64_t req_id) {
    auto tid = session.transaction_id();
    Debug("[%lu] Abort callback", tid);

    auto search = pending_reqs_.find(req_id);
    if (search == pending_reqs_.end()) {
        Debug("[%lu] Transaction already finished", tid);
        return;
    }

    PendingRequest *req = search->second;
    --req->outstandingPrepares;
    if (req->outstandingPrepares == 0) {
        abort_callback acb = req->acb;
        pending_reqs_.erase(req_id);
        delete req;

        rss::EndTransaction(service_name_, session);

        Debug("[%lu] Abort finished", tid);
        acb();
    }
}

/* Commits RO transaction. */
void Client::ROCommit(Session &s, const std::unordered_set<std::string> &keys,
                      commit_callback ccb, commit_timeout_callback ctcb,
                      uint32_t timeout) {
    auto &session = static_cast<StrongSession &>(s);

    auto tid = session.transaction_id();

    Debug("[%lu] ROCOMMIT", tid);

    auto &min_read_ts = session.min_read_ts();
    Debug("[%lu] min_read_ts: %lu.%lu", tid, min_read_ts.getTimestamp(), min_read_ts.getID());

    session.set_committing();

    auto &participants = session.participants();
    ASSERT(participants.size() == 0);

    std::unordered_map<int, std::vector<std::string>> sharded_keys;
    for (auto &key : keys) {
        int i = (*part_)(key, nshards_, -1, participants);
        sharded_keys[i].push_back(key);
        session.add_participant(i);
    }

    uint64_t req_id = last_req_id_++;
    PendingRequest *req = new PendingRequest(req_id);
    pending_reqs_[req_id] = req;
    req->ccb = ccb;
    req->ctcb = ctcb;
    req->outstandingPrepares = sharded_keys.size();

    stats.IncrementList("txn_groups", sharded_keys.size());

    ASSERT(sharded_keys.size() > 0);

    Timestamp min_ts = session.min_read_ts();
    Timestamp commit_ts{tt_.Now().latest(), client_id_};

    // Hack to make RSS work with zero TrueTime error despite clock skew
    // (for throughput experiments)
    if (consistency_ == RSS && min_ts >= commit_ts) {
        commit_ts.setTimestamp(min_ts.getTimestamp() + 1);
    }

    Debug("[%lu] commit_ts: %lu.%lu", tid, commit_ts.getTimestamp(), commit_ts.getID());
    Debug("[%lu] min_ts: %lu.%lu", tid, min_ts.getTimestamp(), min_ts.getID());

    auto roccb = std::bind(&Client::ROCommitCallback, this, std::ref(session), req->id,
                           std::placeholders::_1, std::placeholders::_2,
                           std::placeholders::_3);
    auto rocscb = std::bind(&Client::ROCommitSlowCallback, this, std::ref(session), req->id,
                            std::placeholders::_1, std::placeholders::_2,
                            std::placeholders::_3, std::placeholders::_4);
    auto roctcb = []() {};  // TODO: Handle timeout

    for (auto &s : sharded_keys) {
        sclients_[s.first]->ROCommit(tid, s.second, commit_ts, min_ts, roccb, rocscb, roctcb, timeout);
    }
}

void Client::ROCommitCallback(StrongSession &session, uint64_t req_id, int shard_idx,
                              const std::vector<Value> &values,
                              const std::vector<PreparedTransaction> &prepares) {
    auto tid = session.transaction_id();

    Debug("[%lu] ROCommit callback", tid);

    auto search = pending_reqs_.find(req_id);
    if (search == pending_reqs_.end()) {
        Debug("[%lu] ROCommitCallback for terminated request id %lu", tid, req_id);
        return;
    }

    SnapshotResult r = ReceiveFastPath(session, tid, shard_idx, values, prepares);
    if (r.state == COMMIT) {
        PendingRequest *req = search->second;

        commit_callback ccb = req->ccb;
        pending_reqs_.erase(search);
        delete req;

        session.advance_min_read_ts(r.max_read_ts);

        auto &min_read_ts = session.min_read_ts();
        Debug("min_read_timestamp_: %lu.%lu", min_read_ts.getTimestamp(), min_read_ts.getID());

        rss::EndTransaction(service_name_, session);

        Debug("[%lu] COMMIT OK", tid);
        ccb(COMMITTED);
    } else if (r.state == WAIT) {
        Debug("[%lu] Waiting for more RO responses", tid);
    }
}

void Client::ROCommitSlowCallback(StrongSession &session, uint64_t req_id, int shard_idx,
                                  uint64_t rw_transaction_id, const Timestamp &commit_ts, bool is_commit) {
    auto search = pending_reqs_.find(req_id);
    if (search == pending_reqs_.end()) {
        Debug("ROCommitSlowCallback for terminated request id %lu", req_id);
        return;
    }

    auto tid = session.transaction_id();

    Debug("[%lu] ROCommitSlow callback", tid);

    SnapshotResult r = ReceiveSlowPath(session, tid, rw_transaction_id, is_commit, commit_ts);
    if (r.state == COMMIT) {
        PendingRequest *req = search->second;

        commit_callback ccb = req->ccb;
        pending_reqs_.erase(search);
        delete req;

        session.advance_min_read_ts(r.max_read_ts);

        auto &min_read_ts = session.min_read_ts();
        Debug("min_read_timestamp_: %lu.%lu", min_read_ts.getTimestamp(), min_read_ts.getID());

        rss::EndTransaction(service_name_, session);

        Debug("[%lu] COMMIT OK", tid);
        ccb(COMMITTED);
    } else if (r.state == WAIT) {
        Debug("[%lu] Waiting for more RO responses", tid);
    }
}

SnapshotResult Client::ReceiveFastPath(StrongSession &session,
                                       uint64_t transaction_id,
                                       int shard_idx,
                                       const std::vector<Value> &values,
                                       const std::vector<PreparedTransaction> &prepares) {
    Debug("[%lu] Received fast path RO response", transaction_id);

    auto &participants = session.mutable_participants();

    ASSERT(participants.count(shard_idx) > 0);
    participants.erase(shard_idx);

    AddValues(session, values);
    AddPrepares(session, prepares);

    // Received all fast path responses
    if (participants.size() == 0) {
        ReceivedAllFastPaths(session);
    }

    return CheckCommit(session);
}

SnapshotResult Client::ReceiveSlowPath(StrongSession &session, uint64_t transaction_id,
                                       uint64_t rw_transaction_id,
                                       bool is_commit, const Timestamp &commit_ts) {
    Debug("[%lu] Received slow path RO response", transaction_id);
    ASSERT(consistency_ == RSS);

    auto &prepares = session.mutable_prepares();

    auto search = prepares.find(rw_transaction_id);
    if (search == prepares.end()) {
        Debug("[%lu] already received commit decision for %lu", transaction_id, rw_transaction_id);
        return {WAIT};
    }

    if (is_commit) {
        const PreparedTransaction &pt = search->second;
        Debug("[%lu] adding writes from prepared transaction: %lu", transaction_id, rw_transaction_id);

        std::vector<Value> values;
        for (auto &write : pt.write_set()) {
            values.emplace_back(rw_transaction_id, commit_ts, write.first, write.second);
        }

        AddValues(session, values);
    }

    prepares.erase(search);

    return CheckCommit(session);
}

void Client::AddValues(StrongSession &session, const std::vector<Value> &vs) {
    auto &values = session.mutable_values();

    for (auto &v : vs) {
        std::list<Value> &l = values[v.key()];

        auto it = l.begin();
        for (; it != l.end(); ++it) {
            Value &v2 = *it;
            if (v2.ts() < v.ts()) {
                break;
            }
        }

        l.insert(it, v);
    }
}

void Client::AddPrepares(StrongSession &session, const std::vector<PreparedTransaction> &ps) {
    auto &prepares = session.mutable_prepares();

    for (auto &p : ps) {
        auto search = prepares.find(p.transaction_id());
        if (search == prepares.end()) {
            prepares.insert(search, {p.transaction_id(), p});
        } else {
            PreparedTransaction &pt = search->second;
            ASSERT(pt.transaction_id() == p.transaction_id());
            pt.update_prepare_ts(p.prepare_ts());
            pt.add_write_set(p.write_set());
        }
    }
}

void Client::ReceivedAllFastPaths(StrongSession &session) {
    FindCommittedKeys(session);
    CalculateSnapshotTimestamp(session);
}

void Client::FindCommittedKeys(StrongSession &session) {
    auto &prepares = session.mutable_prepares();
    auto &values = session.mutable_values();

    if (prepares.size() == 0) {
        return;
    }

    ASSERT(consistency_ == RSS);

    std::vector<Value> to_add;
    for (auto &kv : values) {
        std::list<Value> &l = kv.second;
        for (Value &v : l) {
            uint64_t transaction_id = v.transaction_id();
            auto search = prepares.find(transaction_id);
            if (search != prepares.end()) {
                PreparedTransaction &pt = search->second;
                Debug("adding writes from prepared transaction: %lu", transaction_id);

                for (auto &write : pt.write_set()) {
                    to_add.emplace_back(transaction_id, v.ts(), write.first, write.second);
                }

                prepares.erase(search);
            }
        }
    }

    AddValues(session, to_add);
}

void Client::CalculateSnapshotTimestamp(StrongSession &session) {
    auto &values = session.mutable_values();

    // Find snapshot ts, the minimum timestamp we can use to read all keys
    Timestamp snapshot_ts{0, 0};
    for (auto &kv : values) {
        const std::list<Value> &l = kv.second;
        ASSERT(l.size() > 0);
        const Value &v = l.back();
        if (snapshot_ts < v.ts()) {
            snapshot_ts = v.ts();
        }
    }

    session.set_snapshot_ts(snapshot_ts);
}

SnapshotResult Client::CheckCommit(StrongSession &session) {
    auto &participants = session.participants();
    auto &prepares = session.mutable_prepares();
    auto &values = session.mutable_values();
    auto &snapshot_ts = session.snapshot_ts();

    if (participants.size() > 0) {
        return {WAIT};
    }

    if (consistency_ == SS) {
        return {COMMIT, snapshot_ts};
    }

    for (auto &kv : values) {
        Debug("key: %s", kv.first.c_str());
        std::list<Value> &l = kv.second;
        for (Value &v : l) {
            Debug("value: %lu %lu.%lu %s", v.transaction_id(), v.ts().getTimestamp(), v.ts().getID(), v.val().c_str());
        }
    }

    for (auto &p : prepares) {
        Debug("prepare: %lu %lu.%lu", p.second.transaction_id(), p.second.prepare_ts().getTimestamp(), p.second.prepare_ts().getID());
        for (auto &write : p.second.write_set()) {
            Debug("write: %s %s", write.first.c_str(), write.second.c_str());
        }
    }

    // Find min prepare ts
    Timestamp min_ts = Timestamp::MAX;
    for (auto &p : prepares) {
        const PreparedTransaction &pt = p.second;
        if (pt.prepare_ts() < min_ts) {
            min_ts = pt.prepare_ts();
        }
    }

    Debug("min prepare ts: %lu.%lu", min_ts.getTimestamp(), min_ts.getID());
    Debug("snapshot ts: %lu.%lu", snapshot_ts.getTimestamp(), snapshot_ts.getID());
    Debug("can commit: %d", snapshot_ts < min_ts);

    if (snapshot_ts < min_ts) {
        return {COMMIT, snapshot_ts};
    } else {
        return {WAIT};
    }
}

}  // namespace strongstore
