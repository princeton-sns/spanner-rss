// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/txnstore/lib/txnstore.h:
 *   Interface for a single node transactional store serving as a
 *   server-side backend
 *
 * Copyright 2022 Jeffrey Helt, Matthew Burke, Amit Levy, Wyatt Lloyd
 * Copyright 2013-2015 Irene Zhang <iyzhang@cs.washington.edu>
 *                     Naveen Kr. Sharma <naveenks@cs.washington.edu>
 *                     Dan R. K. Ports  <drkp@cs.washington.edu>
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

#ifndef _TXN_STORE_H_
#define _TXN_STORE_H_

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/stats.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"

class TxnStore
{
public:
    TxnStore();
    virtual ~TxnStore();

    // add key to read set
    virtual int Get(uint64_t id, const std::string &key,
                    std::pair<Timestamp, std::string> &value);

    virtual int Get(uint64_t id, const std::string &key,
                    const Timestamp &timestamp,
                    std::pair<Timestamp, std::string> &value,
                    std::unordered_map<uint64_t, int> &statuses);

    // add key to write set
    virtual int Put(uint64_t id, const std::string &key,
                    const std::string &value);

    virtual int Put(uint64_t id, const std::string &key,
                    const std::string &value, const Timestamp &timestamp,
                    std::unordered_map<uint64_t, int> &statuses);

    // check whether we can commit this transaction (and lock the read/write
    // set)
    virtual int Prepare(uint64_t id, const Transaction &txn,
                        std::unordered_map<uint64_t, int> &statuses);

    virtual int Prepare(uint64_t id, const Transaction &txn,
                        const Timestamp &timestamp, Timestamp &proposed);

    // commit the transaction
    virtual bool Commit(uint64_t id, const Timestamp &ts,
                        std::unordered_map<uint64_t, int> &statuses);

    // abort a running transaction
    virtual void Abort(uint64_t id,
                       std::unordered_map<uint64_t, int> &statuses);

    // load keys
    virtual void Load(const std::string &key, const std::string &value,
                      const Timestamp &timestamp);

    inline Stats &GetStats() { return stats; }

protected:
    Stats stats;
};

#endif /* _TXN_STORE_H_ */
