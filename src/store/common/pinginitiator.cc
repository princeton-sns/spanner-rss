#include "store/common/pinginitiator.h"

#include <sstream>

#include "lib/message.h"

PingInitiator::PingInitiator(PingTransport *pingTransport, Transport *transport,
                             size_t numReplicas)
    : pingTransport(pingTransport),
      transport(transport),
      numReplicas(numReplicas),
      alpha(0.75),
      length(1000) {}

PingInitiator::~PingInitiator() {}

void PingInitiator::StartPings() {
    for (size_t i = 0; i < numReplicas; ++i) {
        SendPing(i);
    }
    transport->Timer(length, [this]() {
        std::set<std::pair<uint64_t, size_t>> sortedEstimates;
        for (const auto &estimate : roundTripEstimates) {
            sortedEstimates.insert(
                std::make_pair(estimate.second, estimate.first));
        }

        orderedReplicas.clear();
        orderedEstimates.clear();
        std::stringstream ss;
        ss << "Replica indices in increasing round trip time order: [";
        for (const auto &sortedEstimate : sortedEstimates) {
            orderedReplicas.push_back(sortedEstimate.second);
            orderedEstimates.push_back(sortedEstimate.first);
            ss << sortedEstimate.second << ", ";
        }
        ss << "].";
        Notice("%s", ss.str().c_str());

        done = true;
    });
}

void PingInitiator::SendPing(size_t replica) {
    ping.set_salt(rd());
    struct timespec start;
    if (clock_gettime(CLOCK_MONOTONIC, &start) < 0) {
        PPanic("Failed to get CLOCK_MONOTONIC");
    }
    outstandingSalts.insert(
        std::make_pair(ping.salt(), std::make_pair(replica, start)));
    pingTransport->SendPing(replica, ping);
}

void PingInitiator::HandlePingResponse(const PingMessage &ping) {
    auto saltItr = outstandingSalts.find(ping.salt());
    if (saltItr != outstandingSalts.end()) {
        struct timespec now;
        if (clock_gettime(CLOCK_MONOTONIC, &now) < 0) {
            PPanic("Failed to get CLOCK_MONOTONIC");
        }

        uint64_t roundTrip = timespec_delta(saltItr->second.second, now);
        Debug("Round trip to replica %lu took %luns.", saltItr->second.first,
              roundTrip);
        auto estimateItr = roundTripEstimates.find(saltItr->second.first);
        if (estimateItr == roundTripEstimates.end()) {
            roundTripEstimates.insert(
                std::make_pair(saltItr->second.first, roundTrip));
        } else {
            estimateItr->second =
                (1 - alpha) * estimateItr->second + alpha * roundTrip;
            Debug("Updated round trip estimate to %lu.", estimateItr->second);
        }

        if (!done) {
            SendPing(saltItr->second.first);
        }
        outstandingSalts.erase(saltItr);
    }
}

uint64_t PingInitiator::timespec_delta(const struct timespec &a,
                                       const struct timespec &b) {
    uint64_t delta;
    delta = b.tv_sec - a.tv_sec;
    delta *= 1000000000ll;
    if (b.tv_nsec < a.tv_nsec) {
        delta -= 1000000000ll;
        delta += (b.tv_nsec + 1000000000ll) - a.tv_nsec;
    } else {
        delta += b.tv_nsec - a.tv_nsec;
    }
    return delta;
}
