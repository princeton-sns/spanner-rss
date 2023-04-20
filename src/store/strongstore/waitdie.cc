/***********************************************************************
 *
 * store/strongstore/waitdie.cc:
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
#include "store/strongstore/waitdie.h"

#include <algorithm>

namespace strongstore
{

    WaitDie::WaitDie() {}

    WaitDie::~WaitDie() {}

    WaitDie::Lock::Lock()
        : state_{UNLOCKED}, min_holder_timestamp_{Timestamp::MAX} {};

    void WaitDie::Lock::AddReadWaiter(uint64_t requester,
                                      const Timestamp &start_timestamp,
                                      const Timestamp &waiting_for)
    {
        std::shared_ptr<Waiter> w = std::make_shared<Waiter>(
            true, false, requester, start_timestamp, waiting_for);

        waiters_.emplace(requester, w);
        wait_q_.push_back(requester);
    }

    void WaitDie::Lock::AddWriteWaiter(uint64_t requester,
                                       const Timestamp &start_timestamp,
                                       const Timestamp &waiting_for)
    {
        std::shared_ptr<Waiter> w = std::make_shared<Waiter>(
            false, true, requester, start_timestamp, waiting_for);

        waiters_.emplace(requester, w);
        wait_q_.push_back(requester);
    }

    void WaitDie::Lock::AddReadWriteWaiter(uint64_t requester,
                                           const Timestamp &start_timestamp,
                                           const Timestamp &waiting_for)
    {
        std::shared_ptr<Waiter> w = std::make_shared<Waiter>(
            true, true, requester, start_timestamp, waiting_for);

        waiters_.emplace(requester, w);
        wait_q_.push_back(requester);
    }

    bool WaitDie::Lock::TryReadWait(uint64_t requester,
                                    const Timestamp &start_timestamp)
    {
        for (uint64_t h : holders_)
        {
            Debug("[%lu] holders: %lu", requester, h);
        }

        auto search = waiters_.find(requester);
        if (search != waiters_.end())
        { // I already have a waiter
            std::shared_ptr<Waiter> w = search->second;
            bool isread = w->isread();
            bool iswrite = w->iswrite();

            // Read is already waiting
            if (isread)
            {
                return true;
            }
            else if (iswrite)
            {
                // Upgrade waiting write to read-write
                w->set_read(true);
                return true;
            }
            else
            {
                NOT_REACHABLE();
            }
        }

        while (!wait_q_.empty())
        {
            uint64_t b = wait_q_.back();
            auto search = waiters_.find(b);
            if (search != waiters_.end())
            {
                std::shared_ptr<Waiter> back = search->second;

                Debug("[%lu] back: %d %lu %lu %lu", requester, back->iswrite(),
                      start_timestamp.getTimestamp(),
                      back->waiting_for().getTimestamp(),
                      back->min_waiter().getTimestamp());

                if (!back->iswrite() && start_timestamp < back->waiting_for())
                {
                    back->add_waiter(requester, start_timestamp);
                    waiters_.emplace(requester, back);
                    return true;
                }
                else if (back->iswrite() &&
                         start_timestamp < back->min_waiter())
                {
                    AddReadWaiter(requester, start_timestamp, back->min_waiter());
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                wait_q_.pop_back();
            }
        }

        Debug("[%lu] wait q empty: %lu <? %lu %d", requester,
              start_timestamp.getTimestamp(), min_holder_timestamp_.getTimestamp(),
              start_timestamp < min_holder_timestamp_);
        if (start_timestamp < min_holder_timestamp_)
        {
            AddReadWaiter(requester, start_timestamp, min_holder_timestamp_);
            return true;
        }
        else
        {
            return false;
        }
    }

    int WaitDie::Lock::TryAcquireReadLock(uint64_t requester,
                                          const Timestamp &start_timestamp)
    {
        // Lock is free
        if (state_ == UNLOCKED)
        {
            Debug("[%lu] unlocked", requester);
            ASSERT(holders_.size() == 0);
            ASSERT(min_holder_timestamp_ == Timestamp::MAX);

            state_ = LOCKED_FOR_READ;
            holders_.insert(requester);
            min_holder_timestamp_ = start_timestamp;

            return REPLY_OK;
        }

        // I already hold the lock
        if (holders_.find(requester) != holders_.end())
        {
            Debug("[%lu] already hold lock", requester);
            if (state_ == LOCKED_FOR_WRITE)
            {
                state_ = LOCKED_FOR_READ_WRITE;
                Debug("[%lu] upgrade to rw lock", requester);
            }

            return REPLY_OK;
        }

        // There is no write waiting, take read lock
        if (state_ == LOCKED_FOR_READ && !isWriteNext())
        {
            holders_.insert(requester);
            min_holder_timestamp_ =
                std::min(min_holder_timestamp_, start_timestamp);

            Debug("[%lu] adding myself as reader: %lu", requester, holders_.size());

            for (uint64_t h : holders_)
            {
                Debug("[%lu] holders: %lu", requester, h);
            }

            return REPLY_OK;
        }

        // Try wait
        if (TryReadWait(requester, start_timestamp))
        {
            Debug("[%lu] Waiting on lock", requester);
            return REPLY_WAIT;
        }

        Debug("[%lu] Die", requester);
        // Die
        return REPLY_FAIL;
    }

    bool WaitDie::Lock::PopWaiter()
    {
        bool modified = false;
        std::size_t nh = holders_.size();

        while (!wait_q_.empty() && nh < 2)
        {
            uint64_t next = wait_q_.front();
            if (nh == 1 && holders_.count(next) == 0)
            {
                break;
            }

            wait_q_.pop_front();

            auto search = waiters_.find(next);
            if (search != waiters_.end())
            {
                std::shared_ptr<Waiter> w = search->second;

                for (uint64_t v : w->waiters())
                {
                    Debug("Waiter: %lu", v);
                }
                Debug("Waiter ts: %lu", w->min_waiter().getTimestamp());

                min_holder_timestamp_ = w->min_waiter();

                bool isread = w->isread();
                bool iswrite = w->iswrite();
                if (iswrite && holders_.count(next) > 0)
                {
                    state_ = LOCKED_FOR_READ_WRITE;
                }
                else if (isread && iswrite)
                {
                    state_ = LOCKED_FOR_READ_WRITE;
                }
                else if (iswrite)
                {
                    state_ = LOCKED_FOR_WRITE;
                }
                else
                {
                    state_ = LOCKED_FOR_READ;
                }

                holders_ = std::move(w->waiters());
                for (uint64_t t : holders_)
                {
                    waiters_.erase(t);
                }
                modified = true;
            }

            nh = holders_.size();
        }

        if (holders_.size() == 0)
        {
            state_ = UNLOCKED;
            min_holder_timestamp_ = Timestamp::MAX;
        }

        return modified;
    }

    void WaitDie::Lock::ReleaseReadLock(uint64_t holder,
                                        std::unordered_set<uint64_t> &notify_rws)
    {
        for (uint64_t h : holders_)
        {
            Debug("[%lu] holders before: %lu", holder, h);
        }

        // Clean up waiter
        auto search = waiters_.find(holder);
        if (search != waiters_.end())
        {
            std::shared_ptr<Waiter> w = search->second;

            Debug("[%lu] Cleaning up waiter: %d %d %lu", holder, w->isread(),
                  w->iswrite(), w->waiters().size());

            if (w->iswrite())
            {
                w->set_read(false);
            }
            else if (w->waiters().size() > 1)
            {
                w->remove_waiter(holder);
                waiters_.erase(search);
            }
            else
            {
                waiters_.erase(search);
            }
        }

        if (holders_.count(holder) > 0 &&
            (state_ == LOCKED_FOR_READ || state_ == LOCKED_FOR_READ_WRITE))
        {
            if (state_ == LOCKED_FOR_READ_WRITE)
            {
                Debug("[%lu] downgrade to w lock", holder);
                state_ = LOCKED_FOR_WRITE;
                return;
            }

            holders_.erase(holder);

            for (uint64_t h : holders_)
            {
                Debug("[%lu] holders after: %lu", holder, h);
            }

            Debug("status: %lu %d %lu", holders_.size(), wait_q_.empty(),
                  holders_.count(wait_q_.front()));

            bool notify = PopWaiter();

            if (notify)
            {
                notify_rws.insert(holders_.begin(), holders_.end());
            }
        }
    }

    void WaitDie::Lock::ReleaseWriteLock(uint64_t holder,
                                         std::unordered_set<uint64_t> &notify_rws)
    {
        for (uint64_t h : holders_)
        {
            Debug("[%lu] holders before: %lu", holder, h);
        }

        // Clean up waiter
        auto search = waiters_.find(holder);
        if (search != waiters_.end())
        {
            std::shared_ptr<Waiter> w = search->second;

            Debug("[%lu] Cleaning up waiter: %d %d %lu", holder, w->isread(),
                  w->iswrite(), w->waiters().size());

            if (w->isread())
            {
                w->set_write(false);
            }
            else
            {
                waiters_.erase(search);
            }
        }

        if (holders_.count(holder) > 0 &&
            (state_ == LOCKED_FOR_WRITE || state_ == LOCKED_FOR_READ_WRITE))
        {
            if (state_ == LOCKED_FOR_READ_WRITE)
            {
                Debug("[%lu] downgrade to r lock", holder);
                state_ = LOCKED_FOR_READ;
                return;
            }

            holders_.erase(holder);

            for (uint64_t h : holders_)
            {
                Debug("[%lu] holders after: %lu", holder, h);
            }

            bool notify = PopWaiter();

            if (notify)
            {
                notify_rws.insert(holders_.begin(), holders_.end());
            }
        }
    }

    bool WaitDie::Lock::TryWriteWait(uint64_t requester,
                                     const Timestamp &start_timestamp)
    {
        bool add_read = false;
        auto search = waiters_.find(requester);
        if (search != waiters_.end())
        { // I already have a waiter
            std::shared_ptr<Waiter> w = search->second;
            bool isread = w->isread();
            bool iswrite = w->iswrite();

            // Write is already waiting
            if (iswrite)
            {
                return true;
            }
            else if (isread && w->waiters().size() == 1)
            {
                // Upgrade waiting read to read-write
                w->set_write(true);
                return true;
            }
            else if (isread)
            {
                Debug("wait as read-write");
                // Wait as read-write
                w->remove_waiter(requester);
                waiters_.erase(search);
                add_read = true;
            }
            else
            {
                NOT_REACHABLE();
            }
        }

        while (!wait_q_.empty())
        {
            uint64_t b = wait_q_.back();
            auto search = waiters_.find(b);
            if (search != waiters_.end())
            {
                std::shared_ptr<Waiter> back = search->second;

                Debug("[%lu] back: %d %lu %lu", requester, back->iswrite(),
                      back->waiting_for().getTimestamp(),
                      back->min_waiter().getTimestamp());

                const Timestamp &min_waiter = back->min_waiter();

                if (start_timestamp <= min_waiter)
                {
                    if (add_read)
                    {
                        AddReadWriteWaiter(requester, start_timestamp, min_waiter);
                    }
                    else
                    {
                        AddWriteWaiter(requester, start_timestamp, min_waiter);
                    }
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                wait_q_.pop_back();
            }
        }

        Debug("[%lu] wait q empty: %lu <=? %lu %d", requester,
              start_timestamp.getTimestamp(), min_holder_timestamp_.getTimestamp(),
              start_timestamp <= min_holder_timestamp_);
        if (start_timestamp <= min_holder_timestamp_)
        {
            AddWriteWaiter(requester, start_timestamp, min_holder_timestamp_);
            return true;
        }
        else
        {
            return false;
        }
    }

    int WaitDie::Lock::TryAcquireWriteLock(uint64_t requester,
                                           const Timestamp &start_timestamp)
    {
        // Lock is free
        if (state_ == UNLOCKED)
        {
            Debug("[%lu] unlocked", requester);
            ASSERT(holders_.size() == 0);
            ASSERT(min_holder_timestamp_ == Timestamp::MAX);

            state_ = LOCKED_FOR_WRITE;
            holders_.insert(requester);
            min_holder_timestamp_ = start_timestamp;

            return REPLY_OK;
        }

        // I already hold the lock
        if (holders_.size() == 1 && holders_.count(requester) > 0)
        {
            Debug("[%lu] already hold lock", requester);
            if (state_ == LOCKED_FOR_READ)
            {
                Debug("[%lu] upgrade to rw lock", requester);
                state_ = LOCKED_FOR_READ_WRITE;
            }

            return REPLY_OK;
        }

        // Try wait
        if (TryWriteWait(requester, start_timestamp))
        {
            Debug("[%lu] Waiting on lock", requester);
            return REPLY_WAIT;
        }

        Debug("[%lu] die", requester);
        // Die
        return REPLY_FAIL;
    }

    bool WaitDie::Lock::isWriteNext()
    {
        while (!wait_q_.empty())
        {
            auto search = waiters_.find(wait_q_.front());
            if (search != waiters_.end())
            {
                return search->second->iswrite();
            }
            else
            {
                wait_q_.pop_front();
            }
        }

        return false;
    }

    const LockState WaitDie::GetLockState(const std::string &lock) const
    {
        auto search = locks_.find(lock);
        if (search == locks_.end())
        {
            return UNLOCKED;
        }

        return search->second.state();
    }

    bool WaitDie::HasReadLock(const std::string &lock, uint64_t requester) const
    {
        auto search = locks_.find(lock);
        if (search == locks_.end())
        {
            return false;
        }

        const Lock &l = search->second;
        if (l.state() != LOCKED_FOR_READ && l.state() != LOCKED_FOR_READ_WRITE)
        {
            return false;
        }

        return l.holders().count(requester) > 0;
    }

    int WaitDie::LockForRead(const std::string &lock, uint64_t requester,
                             const Timestamp &start_timestamp)
    {
        Lock &l = locks_[lock];
        Debug("[%lu] Lock for Read: %s", requester, lock.c_str());

        int ret = l.TryAcquireReadLock(requester, start_timestamp);
        if (ret == REPLY_WAIT)
        {
            waiting_[requester].insert(lock);
        }

        return ret;
    }

    int WaitDie::LockForWrite(const std::string &lock, uint64_t requester,
                              const Timestamp &start_timestamp)
    {
        Lock &l = locks_[lock];

        Debug("[%lu] Lock for Write: %s", requester, lock.c_str());

        int ret = l.TryAcquireWriteLock(requester, start_timestamp);
        if (ret == REPLY_WAIT)
        {
            waiting_[requester].insert(lock);
        }

        return ret;
    }

    bool WaitDie::NotifyRW(const std::string &lock, uint64_t rw)
    {
        waiting_[rw].erase(lock);
        if (waiting_[rw].size() == 0)
        {
            waiting_.erase(rw);
            return true;
        }

        return false;
    }

    void WaitDie::NotifyRWs(const std::string &lock, std::unordered_set<uint64_t> &notify_rws)
    {
        for (auto it = notify_rws.begin(); it != notify_rws.end();)
        {
            if (NotifyRW(lock, *it))
            {
                ++it;
            }
            else
            {
                it = notify_rws.erase(it);
            }
        }
    }

    void WaitDie::ReleaseForRead(const std::string &lock, uint64_t holder,
                                 std::unordered_set<uint64_t> &notify_rws)
    {
        if (locks_.find(lock) == locks_.end())
        {
            return;
        }

        Lock &l = locks_[lock];

        l.ReleaseReadLock(holder, notify_rws);

        NotifyRW(lock, holder);
        NotifyRWs(lock, notify_rws);
    }

    void WaitDie::ReleaseForWrite(const std::string &lock, uint64_t holder,
                                  std::unordered_set<uint64_t> &notify_rws)
    {
        if (locks_.find(lock) == locks_.end())
        {
            return;
        }

        Lock &l = locks_[lock];

        l.ReleaseWriteLock(holder, notify_rws);

        NotifyRW(lock, holder);
        NotifyRWs(lock, notify_rws);
    }
}; // namespace strongstore
