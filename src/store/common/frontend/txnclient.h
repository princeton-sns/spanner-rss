// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/common/frontend/txnclient.h
 *   Client interface for a single replicated shard.
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
 *                Naveen Kr. Sharma <naveenks@cs.washington.edu>
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

#ifndef _TXN_CLIENT_H_
#define _TXN_CLIENT_H_

#include <string>

#include "store/common/frontend/client.h"
#include "store/common/promise.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"

#define DEFAULT_TIMEOUT_MS 250
#define DEFAULT_MULTICAST_TIMEOUT_MS 500

// Timeouts for various operations
#define GET_TIMEOUT 250
#define GET_RETRIES 3
// Only used for QWStore
#define PUT_TIMEOUT 250
#define PREPARE_TIMEOUT 1000
#define PREPARE_RETRIES 5

#define COMMIT_TIMEOUT 1000
#define COMMIT_RETRIES 5

#define ABORT_TIMEOUT 1000
#define RETRY_TIMEOUT 500000

typedef std::function<void(int, Timestamp)> prepare_callback;
typedef std::function<void(int, Timestamp)> prepare_timeout_callback;

class TxnClient {
   public:
    TxnClient() : lastReqId(0UL) {}
    virtual ~TxnClient() {}

    // Begin a transaction.
    virtual void Begin(uint64_t id) = 0;

    // Get the value corresponding to key.
    virtual void Get(uint64_t id, const std::string &key, get_callback gcb,
                     get_timeout_callback gtcb, uint32_t timeout) = 0;
    virtual void Get(uint64_t id, const std::string &key,
                     const Timestamp &timestamp, get_callback gcb,
                     get_timeout_callback gtcb, uint32_t timeout) = 0;

    virtual void GetForUpdate(uint64_t id, const std::string &key,
                              const Timestamp &timestamp, get_callback gcb,
                              get_timeout_callback gtcb, uint32_t timeout) {
        Panic("Not implemented.");
    }

    // Set the value for the given key.
    virtual void Put(uint64_t id, const std::string &key,
                     const std::string &value, put_callback pcb,
                     put_timeout_callback ptcb, uint32_t timeout) = 0;

    virtual void Put(uint64_t id, const std::string &key,
                     const std::string &value, const Timestamp &ts,
                     put_callback pcb, put_timeout_callback ptcb,
                     uint32_t timeout) {
        Panic("Not implemented.");
    }

    // Commit all Get(s) and Put(s) since Begin().
    virtual void Commit(uint64_t id, const Transaction &txn,
                        const Timestamp &ts, commit_callback ccb,
                        commit_timeout_callback ctcb, uint32_t timeout) = 0;

    // Abort all Get(s) and Put(s) since Begin().
    virtual void Abort(uint64_t id, const Transaction &txn, abort_callback acb,
                       abort_timeout_callback atcb, uint32_t timeout) = 0;

    // Prepare the transaction.
    virtual void Prepare(uint64_t id, const Transaction &txn,
                         const Timestamp &timestamp, prepare_callback pcb,
                         prepare_timeout_callback ptcb, uint32_t timeout) = 0;

   protected:
    struct PendingRequest {
        PendingRequest(uint64_t reqId) : reqId(reqId) {}
        uint64_t reqId;
    };
    struct PendingGet : public PendingRequest {
        PendingGet(uint64_t reqId) : PendingRequest(reqId) {}
        std::string key;
        get_callback gcb;
        get_timeout_callback gtcb;
    };
    struct PendingPut : public PendingRequest {
        PendingPut(uint64_t reqId) : PendingRequest(reqId) {}
        std::string key;
        std::string value;
        put_callback pcb;
        put_timeout_callback ptcb;
    };
    struct PendingPrepare : public PendingRequest {
        PendingPrepare(uint64_t reqId) : PendingRequest(reqId) {}
        prepare_callback pcb;
        prepare_timeout_callback ptcb;
    };
    struct PendingCommit : public PendingRequest {
        PendingCommit(uint64_t reqId) : PendingRequest(reqId) {}
        commit_callback ccb;
        commit_timeout_callback ctcb;
    };
    struct PendingAbort : public PendingRequest {
        PendingAbort(uint64_t reqId) : PendingRequest(reqId) {}
        abort_callback acb;
        abort_timeout_callback atcb;
    };

    uint64_t lastReqId;
};

#endif /* _TXN_CLIENT_H_ */
