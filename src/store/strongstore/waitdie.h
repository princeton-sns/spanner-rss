/***********************************************************************
 *
 * store/strongstore/waitdie.h:
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
#ifndef _STRONG_WAIT_DIE_H_
#define _STRONG_WAIT_DIE_H_

#include <sys/time.h>

#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"

namespace strongstore
{

    enum LockState
    {
        UNLOCKED,
        LOCKED_FOR_READ,
        LOCKED_FOR_WRITE,
        LOCKED_FOR_READ_WRITE
    };

    class WaitDie
    {
    public:
        WaitDie();
        ~WaitDie();

        const LockState GetLockState(const std::string &lock) const;
        bool HasReadLock(const std::string &lock, uint64_t requester) const;

        int LockForRead(const std::string &lock, uint64_t requester,
                        const Timestamp &start_timestamp);
        int LockForWrite(const std::string &lock, uint64_t requester,
                         const Timestamp &start_timestamp);
        void ReleaseForRead(const std::string &lock, uint64_t holder,
                            std::unordered_set<uint64_t> &notify_rws);
        void ReleaseForWrite(const std::string &lock, uint64_t holder,
                             std::unordered_set<uint64_t> &notify_rws);

    private:
        class Waiter
        {
        public:
            Waiter() : write_{false} {}
            Waiter(bool r, bool w, uint64_t waiter,
                   const Timestamp &start_timestamp, const Timestamp &waiting_for)
                : waiters_{},
                  min_waiter_{Timestamp::MAX},
                  waiting_for_{waiting_for},
                  write_{w},
                  read_{r}
            {
                add_waiter(waiter, start_timestamp);
            }

            bool isread() const { return read_; }
            void set_read(bool r) { read_ = r; }

            bool iswrite() const { return write_; }
            void set_write(bool w) { write_ = w; }

            const Timestamp &min_waiter() const { return min_waiter_; }
            const Timestamp &waiting_for() const { return waiting_for_; }

            void add_waiter(uint64_t w, const Timestamp &ts)
            {
                waiters_.insert(w);
                min_waiter_ = std::min(min_waiter_, ts);
            }

            void remove_waiter(uint64_t w) { waiters_.erase(w); }

            const std::unordered_set<uint64_t> &waiters() const { return waiters_; }

        private:
            std::unordered_set<uint64_t> waiters_;
            Timestamp min_waiter_;
            Timestamp waiting_for_;
            bool write_;
            bool read_;
        };

        class Lock
        {
        public:
            Lock();

            int TryAcquireReadLock(uint64_t requester,
                                   const Timestamp &start_timestamp);
            int TryAcquireWriteLock(uint64_t requester,
                                    const Timestamp &start_timestamp);

            void ReleaseReadLock(uint64_t holder,
                                 std::unordered_set<uint64_t> &notify_rws);
            void ReleaseWriteLock(uint64_t holder,
                                  std::unordered_set<uint64_t> &notify_rws);

            const LockState state() const { return state_; }

            const std::unordered_set<uint64_t> &holders() const { return holders_; };

        private:
            LockState state_;
            std::unordered_set<uint64_t> holders_;
            std::deque<uint64_t> wait_q_;
            std::unordered_map<uint64_t, std::shared_ptr<Waiter>> waiters_;
            Timestamp min_holder_timestamp_;

            bool isWriteNext();

            bool TryReadWait(uint64_t requester, const Timestamp &start_timestamp);
            bool TryWriteWait(uint64_t requester, const Timestamp &start_timestamp);

            void AddReadWaiter(uint64_t requester, const Timestamp &start_timestamp,
                               const Timestamp &waiting_for);
            void AddWriteWaiter(uint64_t requester,
                                const Timestamp &start_timestamp,
                                const Timestamp &waiting_for);
            void AddReadWriteWaiter(uint64_t requester,
                                    const Timestamp &start_timestamp,
                                    const Timestamp &waiting_for);

            bool PopWaiter();
        };

        bool NotifyRW(const std::string &lock, uint64_t rw);
        void NotifyRWs(const std::string &lock, std::unordered_set<uint64_t> &notify_rws);

        /* Global store which keep key -> (timestamp, value) list. */
        std::unordered_map<std::string, Lock> locks_;
        std::unordered_map<uint64_t, std::unordered_set<std::string>> waiting_;
    };

}; // namespace strongstore

#endif /* _STRONG_WAIT_DIE_H_ */
