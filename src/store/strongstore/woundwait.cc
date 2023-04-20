/***********************************************************************
 *
 * store/strongstore/woundwait.cc:
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
#include "store/strongstore/woundwait.h"

#include <algorithm>

namespace strongstore
{

    WoundWait::WoundWait() {}

    WoundWait::~WoundWait() {}

    WoundWait::Lock::Lock() : state_{UNLOCKED} {};

    void WoundWait::Lock::AddReadWaiter(uint64_t requester, const Timestamp &ts)
    {
        std::shared_ptr<Waiter> w =
            std::make_shared<Waiter>(true, false, requester, ts);

        waiters_.emplace(requester, w);
        wait_q_.push_back(requester);
    }

    void WoundWait::Lock::AddWriteWaiter(uint64_t requester, const Timestamp &ts)
    {
        std::shared_ptr<Waiter> w =
            std::make_shared<Waiter>(false, true, requester, ts);

        waiters_.emplace(requester, w);
        wait_q_.push_back(requester);
    }

    void WoundWait::Lock::AddReadWriteWaiter(uint64_t requester,
                                             const Timestamp &ts)
    {
        std::shared_ptr<Waiter> w =
            std::make_shared<Waiter>(true, true, requester, ts);

        waiters_.emplace(requester, w);
        wait_q_.push_back(requester);
    }

    void WoundWait::Lock::ReadWait(uint64_t requester, const Timestamp &ts,
                                   std::unordered_set<uint64_t> &wound)
    {
        auto search = waiters_.find(requester);
        if (search != waiters_.end())
        { // I already have a waiter
            std::shared_ptr<Waiter> w = search->second;
            w->set_read(true);
            Debug("[%lu] I already have a waiter", requester);
            return;
        }

        // Wound other waiters
        for (auto it = wait_q_.begin(); it != wait_q_.end();)
        {
            uint64_t h = *it;

            // Waiter already released lock
            auto search = waiters_.find(h);
            if (search == waiters_.end())
            {
                it = wait_q_.erase(it);
                continue;
            }

            std::shared_ptr<Waiter> waiter = search->second;

            if (waiter->iswrite())
            { // No need to wound other readers
                for (auto w : waiter->waiters())
                {
                    if (w.first != requester && ts < w.second)
                    {
                        wound.insert(w.first);
                    }
                }
            }

            ++it;
        }

        // Wound holders
        if (state_ == LOCKED_FOR_WRITE || state_ == LOCKED_FOR_READ_WRITE)
        {
            for (auto w : holders_)
            {
                if (w.first != requester && ts < w.second)
                {
                    wound.insert(w.first);
                }
            }
        }

        // Add waiter
        AddReadWaiter(requester, ts);
    }

    int WoundWait::Lock::TryAcquireReadLock(uint64_t requester, const Timestamp &ts,
                                            std::unordered_set<uint64_t> &wound)
    {
        // Lock is free
        if (state_ == UNLOCKED)
        {
            ASSERT(holders_.size() == 0);

            state_ = LOCKED_FOR_READ;
            holders_[requester] = ts;

            return REPLY_OK;
        }

        // I already hold the lock
        if (holders_.find(requester) != holders_.end())
        {
            if (state_ == LOCKED_FOR_WRITE)
            {
                state_ = LOCKED_FOR_READ_WRITE;
            }

            return REPLY_OK;
        }

        // There is no write waiting, take read lock
        if (state_ == LOCKED_FOR_READ && !isWriteNext())
        {
            holders_[requester] = ts;

            return REPLY_OK;
        }

        // Wait (and possibly wound)
        ReadWait(requester, ts, wound);
        return REPLY_WAIT;
    }

    void WoundWait::Lock::PopWaiter(std::unordered_set<uint64_t> &notify)
    {
        // for (auto it = wait_q_.begin(); it != wait_q_.end(); ++it) {
        //     Debug("wait_q_: %lu", *it);
        // }

        while (!wait_q_.empty())
        {
            uint64_t next = wait_q_.front();
            auto search = waiters_.find(next);
            if (search == waiters_.end())
            {
                wait_q_.pop_front();
                continue;
            }

            std::size_t nh = holders_.size();
            std::shared_ptr<Waiter> w = search->second;
            bool isread = w->isread();
            bool iswrite = w->iswrite();

            if (nh == 0)
            {
                wait_q_.pop_front();
                if (isread && iswrite)
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
                waiters_.erase(search); // If next not in waiters anymore
                for (auto h : holders_)
                {
                    waiters_.erase(h.first);
                }

                for (auto h : holders_)
                {
                    Debug("added notify1: %lu", h.first);
                    notify.insert(h.first);
                }
            }
            else if (state_ == LOCKED_FOR_READ)
            {
                if (nh == 1 && iswrite && holders_.count(next) > 0)
                {
                    wait_q_.pop_front();
                    waiters_.erase(search);
                    Debug("added notify2: %lu", next);
                    notify.insert(next);
                    // Merge into rw lock
                    state_ = LOCKED_FOR_READ_WRITE;
                }
                else if (!iswrite)
                {
                    wait_q_.pop_front();
                    waiters_.erase(search); // If next not in waiters anymore
                    for (auto h : w->waiters())
                    {
                        holders_.insert(h);
                        Debug("added notify3: %lu", h.first);
                        notify.insert(h.first);
                        waiters_.erase(h.first);
                    }
                }
                else
                {
                    break;
                }
            }
            else if (state_ == LOCKED_FOR_WRITE ||
                     state_ == LOCKED_FOR_READ_WRITE)
            {
                break;
            }
        }

        if (holders_.size() == 0)
        {
            state_ = UNLOCKED;
        }
    }

    void WoundWait::Lock::ReleaseReadLock(uint64_t holder,
                                          std::unordered_set<uint64_t> &notify)
    {
        // Clean up waiter
        auto search = waiters_.find(holder);
        if (search != waiters_.end())
        {
            std::shared_ptr<Waiter> w = search->second;

            if (w->iswrite())
            {
                w->set_read(false);
            }
            else if (w->waiters().size() > 1)
            {
                w->remove_waiter(holder);
            }
            else
            {
                uint64_t first_waiter = w->first_waiter();
                waiters_.erase(search);
                waiters_.erase(first_waiter);
            }
        }

        if (holders_.count(holder) > 0 &&
            (state_ == LOCKED_FOR_READ || state_ == LOCKED_FOR_READ_WRITE))
        {
            if (state_ == LOCKED_FOR_READ_WRITE)
            {
                state_ = LOCKED_FOR_WRITE;
                return;
            }

            holders_.erase(holder);
        }

        PopWaiter(notify);
    }

    void WoundWait::Lock::ReleaseWriteLock(uint64_t holder,
                                           std::unordered_set<uint64_t> &notify)
    {
        // Clean up waiter
        auto search = waiters_.find(holder);
        if (search != waiters_.end())
        {
            std::shared_ptr<Waiter> w = search->second;

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
                state_ = LOCKED_FOR_READ;
                return;
            }

            holders_.erase(holder);
        }

        PopWaiter(notify);
    }

    void WoundWait::Lock::WriteWait(uint64_t requester, const Timestamp &ts,
                                    std::unordered_set<uint64_t> &wound)
    {
        bool read_waiting = false;
        bool safe_upgrade_rw = false;
        auto search = waiters_.find(requester);
        if (search != waiters_.end())
        { // I already have a waiter
            std::shared_ptr<Waiter> w = search->second;

            if (w->iswrite())
            {
                return;
            }

            read_waiting = true;
            safe_upgrade_rw = w->waiters().size() == 1;
            for (auto it = wait_q_.rbegin();
                 safe_upgrade_rw && it != wait_q_.rend(); ++it)
            {
                uint64_t h = *it;

                auto search = waiters_.find(h);
                if (search == waiters_.end())
                {
                    continue;
                }

                std::shared_ptr<Waiter> waiter = search->second;
                if (waiter->waiters().count(requester) > 0)
                {
                    break;
                }

                for (auto w : waiter->waiters())
                {
                    safe_upgrade_rw = safe_upgrade_rw && (ts < w.second);
                }
            }

            if (safe_upgrade_rw)
            {
                w->set_write(true);
            }
            else
            {
                w->remove_waiter(requester);
                waiters_.erase(search);
            }
        }

        // Wound other waiters
        for (auto it = wait_q_.begin(); it != wait_q_.end();)
        {
            uint64_t h = *it;

            // Waiter already released lock
            auto search = waiters_.find(h);
            if (search == waiters_.end())
            {
                it = wait_q_.erase(it);
                continue;
            }

            std::shared_ptr<Waiter> waiter = search->second;

            if (waiter->waiters().count(requester) > 0)
            {
                break;
            }

            bool already_wounded = read_waiting && waiter->iswrite();

            if (!already_wounded)
            {
                for (auto w : waiter->waiters())
                {
                    if (w.first != requester && ts < w.second)
                    {
                        wound.insert(w.first);
                    }
                }
            }

            ++it;
        }

        // Wound holders
        if (!read_waiting || state_ == LOCKED_FOR_READ)
        {
            for (auto w : holders_)
            {
                if (w.first != requester && ts < w.second)
                {
                    wound.insert(w.first);
                }
            }
        }

        // Add waiter
        if (!safe_upgrade_rw)
        {
            if (read_waiting)
            {
                AddReadWriteWaiter(requester, ts);
            }
            else
            {
                AddWriteWaiter(requester, ts);
            }
        }
    }

    bool WoundWait::Lock::SafeUpgradeToRW(const Timestamp &ts)
    {
        for (auto it = wait_q_.cbegin(); it != wait_q_.cend(); ++it)
        {
            uint64_t h = *it;

            // Waiter already released lock
            auto search = waiters_.find(h);
            if (search == waiters_.end())
            {
                it = wait_q_.erase(it);
                continue;
            }

            std::shared_ptr<Waiter> waiter = search->second;

            for (auto w : waiter->waiters())
            {
                if (ts < w.second)
                {
                    return false;
                }
            }
        }

        return true;
    }

    int WoundWait::Lock::TryAcquireWriteLock(uint64_t requester,
                                             const Timestamp &ts,
                                             std::unordered_set<uint64_t> &wound)
    {
        // Lock is free
        if (state_ == UNLOCKED)
        {
            ASSERT(holders_.size() == 0);

            state_ = LOCKED_FOR_WRITE;
            holders_[requester] = ts;

            return REPLY_OK;
        }

        // I already hold the lock
        if (holders_.size() == 1 && holders_.count(requester) > 0)
        {
            if (state_ == LOCKED_FOR_WRITE || state_ == LOCKED_FOR_READ_WRITE)
            {
                return REPLY_OK;
            }

            if (state_ == LOCKED_FOR_READ && SafeUpgradeToRW(ts))
            {
                state_ = LOCKED_FOR_READ_WRITE;
                return REPLY_OK;
            }
        }

        // Wait (and possibly wound)
        WriteWait(requester, ts, wound);
        return REPLY_WAIT;
    }

    bool WoundWait::Lock::isWriteNext()
    {
        while (!wait_q_.empty())
        {
            auto search = waiters_.find(wait_q_.front());
            if (search == waiters_.end())
            {
                wait_q_.pop_front();
                continue;
            }

            return search->second->iswrite();
        }

        return false;
    }

    LockState WoundWait::GetLockState(const std::string &lock) const
    {
        auto search = locks_.find(lock);
        if (search == locks_.end())
        {
            return UNLOCKED;
        }

        return search->second.state();
    }

    bool WoundWait::HasReadLock(const std::string &lock, uint64_t requester) const
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

    int WoundWait::LockForRead(const std::string &lock, uint64_t requester,
                               const Timestamp &ts,
                               std::unordered_set<uint64_t> &wound)
    {
        Lock &l = locks_[lock];
        Debug("[%lu] Lock for Read: %s", requester, lock.c_str());

        int ret = l.TryAcquireReadLock(requester, ts, wound);

        return ret;
    }

    int WoundWait::LockForWrite(const std::string &lock, uint64_t requester,
                                const Timestamp &ts,
                                std::unordered_set<uint64_t> &wound)
    {
        Lock &l = locks_[lock];

        Debug("[%lu] Lock for Write: %s", requester, lock.c_str());

        int ret = l.TryAcquireWriteLock(requester, ts, wound);

        return ret;
    }

    void WoundWait::ReleaseForRead(const std::string &lock, uint64_t holder,
                                   std::unordered_set<uint64_t> &notify)
    {
        if (locks_.find(lock) == locks_.end())
        {
            return;
        }

        Lock &l = locks_[lock];

        l.ReleaseReadLock(holder, notify);
    }

    void WoundWait::ReleaseForWrite(const std::string &lock, uint64_t holder,
                                    std::unordered_set<uint64_t> &notify)
    {
        if (locks_.find(lock) == locks_.end())
        {
            return;
        }

        Lock &l = locks_[lock];

        l.ReleaseWriteLock(holder, notify);
    }
}; // namespace strongstore
