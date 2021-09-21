#ifndef RETWIS_CLIENT_H
#define RETWIS_CLIENT_H

#include <cstdint>
#include <string>
#include <vector>

#include "store/benchmark/async/bench_client.h"
#include "store/benchmark/async/common/key_selector.h"
#include "store/benchmark/async/retwis/retwis_transaction.h"
#include "store/common/frontend/client.h"

namespace retwis {

enum KeySelection {
    UNIFORM,
    ZIPF
};

class RetwisClient : public BenchmarkClient {
   public:
    RetwisClient(KeySelector *keySelector, const std::vector<Client *> &clients, uint32_t timeout,
                 Transport &transport, uint64_t id,
                 BenchmarkClientMode mode,
                 double switch_probability,
                 double arrival_rate, double think_time, double stay_probability,
                 int mpl,
                 int expDuration, int warmupSec, int cooldownSec, int tputInterval, uint32_t abortBackoff,
                 bool retryAborted, uint32_t maxBackoff, uint32_t maxAttempts,
                 const std::string &latencyFilename = "latency");

    virtual ~RetwisClient();

   protected:
    virtual AsyncTransaction *GetNextTransaction() override;

   private:
    KeySelector *keySelector;
    std::string lastOp;
};

}  //namespace retwis

#endif /* RETWIS_CLIENT_H */
