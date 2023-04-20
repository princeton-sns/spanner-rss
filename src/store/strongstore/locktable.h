/***********************************************************************
 *
 * store/strongstore/locktable.h:
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
#ifndef _STRONG_LOCK_TABLE_H_
#define _STRONG_LOCK_TABLE_H_

#include <cstdint>
#include <string>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/transaction.h"
#include "store/strongstore/woundwait.h"

namespace strongstore
{

    enum LockStatus
    {
        ACQUIRED,
        WAITING,
        FAIL
    };

    struct LockAcquireResult
    {
        LockStatus status;
        std::unordered_set<uint64_t> wound_rws;
    };

    struct LockReleaseResult
    {
        std::unordered_set<uint64_t> notify_rws;
    };

    class LockTable
    {
    public:
        LockTable();
        ~LockTable();

        bool HasReadLock(uint64_t transaction_id, const std::string &key);
        LockAcquireResult AcquireReadLock(uint64_t transaction_id,
                                          const Timestamp &ts,
                                          const std::string &key);
        LockAcquireResult AcquireReadWriteLock(uint64_t transaction_id,
                                               const Timestamp &ts,
                                               const std::string &key);

        LockAcquireResult AcquireLocks(uint64_t transaction_id,
                                       const Transaction &transaction);
        LockReleaseResult ReleaseLocks(uint64_t transaction_id,
                                       const Transaction &transaction);

    private:
        WoundWait locks_;

        LockStatus ConvertToResultStatus(int status);
    };

} // namespace strongstore

#endif /* _STRONG_LOCK_TABLE_H_ */
