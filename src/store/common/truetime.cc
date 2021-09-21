// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * common/truetime.cc:
 *   A simulated TrueTime module
 *
 **********************************************************************/

#include "store/common/truetime.h"

#include <chrono>

TrueTime::TrueTime() : error_{0} {}

TrueTime::TrueTime(uint64_t error_ms) : error_{error_ms * 1000} {
    Debug("TrueTime variance: error_ms=%lu", error_ms);
}

uint64_t TrueTime::GetTime() const {
    auto now = std::chrono::high_resolution_clock::now();
    long count = std::chrono::duration_cast<std::chrono::microseconds>(
                     now.time_since_epoch())
                     .count();

    return static_cast<uint64_t>(count);
}

TrueTimeInterval TrueTime::Now() const {
    uint64_t time = GetTime();
    return {time - error_, time + error_};
}

uint64_t TrueTime::TimeToWaitUntilMS(uint64_t ts) const {
    auto now = Now();
    uint64_t earliest = now.earliest();
    if (ts <= earliest) {
        return 0;
    } else {
        return (ts - earliest) / 1000;
    }
}

uint64_t TrueTime::TimeToWaitUntilMicros(uint64_t ts) const {
    auto now = Now();
    uint64_t earliest = now.earliest();
    if (ts <= earliest) {
        return 0;
    } else {
        return ts - earliest;
    }
}