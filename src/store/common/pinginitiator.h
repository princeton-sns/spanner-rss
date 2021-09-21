#ifndef PING_INITIATOR_H
#define PING_INITIATOR_H

#include <ctime>
#include <map>
#include <random>

#include "lib/transport.h"
#include "store/common/common-proto.pb.h"

class PingTransport {
   public:
    PingTransport() {}
    virtual ~PingTransport() {}

    virtual bool SendPing(size_t replica, const PingMessage &ping) = 0;
};

class PingInitiator {
   public:
    PingInitiator(PingTransport *pingTransport, Transport *transport,
                  size_t numReplicas);
    virtual ~PingInitiator();

    void StartPings();

   protected:
    inline const std::vector<size_t> &GetOrderedReplicas() const {
        return orderedReplicas;
    }

    inline const std::vector<uint64_t> &GetOrderedEstimates() const {
        return orderedEstimates;
    }

    inline bool Done() const { return done; }

    void HandlePingResponse(const PingMessage &ping);

   private:
    void SendPing(size_t replica);

    static uint64_t timespec_delta(const struct timespec &a,
                                   const struct timespec &b);

    PingTransport *pingTransport;
    Transport *transport;
    const size_t numReplicas;
    const double alpha;
    const uint64_t length;

    bool done;
    std::mt19937 rd;
    std::map<uint64_t, std::pair<size_t, struct timespec>> outstandingSalts;
    std::map<size_t, uint64_t> roundTripEstimates;

    std::vector<size_t> orderedReplicas;
    std::vector<uint64_t> orderedEstimates;

    PingMessage ping;
};

#endif /* PING_INITIATOR_H */
