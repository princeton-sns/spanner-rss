#ifndef OPEN_BENCHMARK_CLIENT_H
#define OPEN_BENCHMARK_CLIENT_H

#include <functional>
#include <memory>
#include <random>
#include <unordered_map>
#include <vector>

#include "lib/latency.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "store/common/frontend/async_transaction.h"
#include "store/common/frontend/client.h"
#include "store/common/stats.h"
#include "store/common/transaction.h"

typedef std::function<void(transaction_status_t)> execute_callback;

typedef std::function<void()> bench_done_callback;

enum BenchmarkClientMode {
    UNKNOWN,
    OPEN,
    CLOSED
};

class BenchmarkClient {
   public:
    BenchmarkClient(const std::vector<Client *> &clients, uint32_t timeout,
                    Transport &transport, uint64_t id,
                    BenchmarkClientMode mode,
                    double switch_probability,
                    double arrival_rate, double think_time, double stay_probability,
                    int mpl,
                    int expDuration, int warmupSec, int cooldownSec,
                    uint32_t abortBackoff, bool retryAborted,
                    uint32_t maxBackoff, uint32_t maxAttempts,
                    const std::string &latencyFilename = "");
    virtual ~BenchmarkClient();

    void Start(bench_done_callback bdcb);
    void OnReply(uint64_t transaction_id, int result, bool erase_session);

    void SendNext();
    void ExecuteCallback(uint64_t transaction_id, transaction_status_t result);

    inline bool IsFullyDone() { return done; }

    struct Latency_t latency;
    std::vector<uint64_t> latencies;

    inline const Stats &GetStats() const { return stats; }

   protected:
    virtual AsyncTransaction *GetNextTransaction() = 0;

    inline std::mt19937 &GetRand() { return rand_; }

    enum BenchState { WARM_UP = 0,
                      MEASURE = 1,
                      COOL_DOWN = 2,
                      DONE = 3 };
    BenchState GetBenchState(struct timeval &diff) const;
    BenchState GetBenchState() const;

    Stats stats;
    Transport &transport_;

   private:
    class SessionState {
       public:
        SessionState(Session &session, AsyncTransaction *transaction, execute_callback ecb, std::size_t client_index)
            : lat_{}, session_{session}, transaction_{transaction}, ecb_{ecb}, n_attempts_{1}, op_index_{1}, current_client_index_{client_index}, current_client_txn_count_{0} {}

        Session &session() { return session_; }
        AsyncTransaction *transaction() const { return transaction_; }
        execute_callback ecb() const { return ecb_; }

        Latency_Frame_t *lat() { return &lat_; }

        uint64_t n_attempts() const { return n_attempts_; }

        uint64_t op_index() const { return op_index_; }
        void incr_op_index() { op_index_++; }

        std::size_t current_client_index() const { return current_client_index_; }

        void start_transaction(Session &session, AsyncTransaction *transaction, execute_callback ecb, std::size_t client_index) {
            session_ = session;
            transaction_ = transaction;
            ecb_ = ecb;
            current_client_index_ = client_index;
            n_attempts_ = 1;
            op_index_ = 1;
        }

        void retry_transaction() {
            n_attempts_++;
            op_index_ = 1;
        }

       private:
        Latency_Frame_t lat_;
        std::reference_wrapper<Session> session_;
        AsyncTransaction *transaction_;
        execute_callback ecb_;
        uint64_t n_attempts_;
        std::size_t op_index_;
        std::size_t current_client_index_;
        std::size_t current_client_txn_count_;
    };

    void ExecuteAbort(const uint64_t session_id, transaction_status_t status);

    void SendNextInSession(const uint64_t session_id);

    void ExecuteNextOperation(const uint64_t session_id);

    void GetCallback(const uint64_t session_id,
                     int status, const std::string &key, const std::string &val, Timestamp ts);
    void GetTimeout(const uint64_t session_id,
                    int status, const std::string &key);

    void PutCallback(const uint64_t session_id,
                     int status, const std::string &key, const std::string &val);
    void PutTimeout(const uint64_t session_id,
                    int status, const std::string &key, const std::string &val);

    void CommitCallback(const uint64_t session_id, transaction_status_t status);
    void CommitTimeout();
    void AbortCallback(const uint64_t session_id, transaction_status_t status);
    void AbortTimeout();

    void Finish();
    void WarmupDone();
    void CooldownDone();
    void Cleanup();
    void CleanupContinue();

    std::unordered_map<uint64_t, SessionState> session_states_;

    const std::vector<Client *> &clients_;

    const uint64_t client_id_;
    uint32_t timeout_;
    std::mt19937 rand_;
    std::exponential_distribution<> next_arrival_dist_;
    std::exponential_distribution<> think_time_dist_;
    std::bernoulli_distribution stay_dist_;
    std::bernoulli_distribution switch_dist_;
    int n;
    int n_sessions_started_;
    int mpl_;
    int exp_duration_;
    int warmupSec;
    int cooldownSec;
    struct timeval startTime;
    struct timeval endTime;
    struct timeval startMeasureTime;
    string latencyFilename;
    int msSinceStart;
    bench_done_callback curr_bdcb_;

    uint64_t maxBackoff;
    uint64_t abortBackoff;
    bool retryAborted;
    int64_t maxAttempts;

    bool started;
    bool done;
    bool cooldownStarted;

    BenchmarkClientMode mode_;
};

#endif /* OPEN_BENCHMARK_CLIENT_H */
