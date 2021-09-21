// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * common/truetime.h
 *   A simulated TrueTime module
 *
 **********************************************************************/

#ifndef _TRUETIME_H_
#define _TRUETIME_H_

#include "lib/message.h"

class TrueTimeInterval {
   public:
    TrueTimeInterval(uint64_t earliest, uint64_t latest)
        : earliest_{earliest}, latest_{latest} {}

    ~TrueTimeInterval() {}

    uint64_t earliest() const { return earliest_; }
    uint64_t latest() const { return latest_; }
    uint64_t mid() const { return (latest_ + earliest_) / 2; }

   private:
    uint64_t earliest_;
    uint64_t latest_;
};

class TrueTime {
   public:
    TrueTime();
    TrueTime(uint64_t error);
    ~TrueTime(){};

    uint64_t GetTime() const;

    TrueTimeInterval Now() const;

    uint64_t TimeToWaitUntilMS(uint64_t ts) const;
    uint64_t TimeToWaitUntilMicros(uint64_t ts) const;

   private:
    uint64_t error_;
};

#endif /* _TRUETIME_H_ */
