// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/common/frontend/bufferclient.cc:
 *   Single shard buffering client implementation.
 *
 * Copyright 2022 Jeffrey Helt, Matthew Burke, Amit Levy, Wyatt Lloyd
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
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

#include "store/common/frontend/bufferclient.h"

using namespace std;

BufferClient::BufferClient(TxnClient *txnclient, bool bufferPuts)
    : txnclient(txnclient), bufferPuts(bufferPuts) {}

BufferClient::~BufferClient() {}

/* Begins a transaction. */
void BufferClient::Begin(uint64_t tid)
{
    // Initialize data structures.
    txn = Transaction();
    readSet.clear();
    this->tid = tid;
    txnclient->Begin(tid);
}

void BufferClient::Get(const std::string &key, get_callback gcb,
                       get_timeout_callback gtcb, uint32_t timeout)
{
    // Read your own writes, check the write set first.
    if (txn.getWriteSet().find(key) != txn.getWriteSet().end())
    {
        gcb(REPLY_OK, key, (txn.getWriteSet().find(key))->second, Timestamp());
        return;
    }

    // Consistent reads, check the read set.
    if (txn.getReadSet().find(key) != txn.getReadSet().end())
    {
        auto readSetItr = readSet.find(key);
        ASSERT(readSetItr != readSet.end());
        gcb(REPLY_OK, key, std::get<0>(readSetItr->second),
            std::get<1>(readSetItr->second));
        return;
    }

    get_callback bufferCb = [this, gcb](int status, const std::string &key,
                                        const std::string &value,
                                        Timestamp ts)
    {
        // TODO: we still need to add a "failed" read to the read set
        //   (where failed ==> successful rpc, but no value exists for key)
        // if (status == REPLY_OK) {
        Debug("Added to %lu to read set.", ts.getTimestamp());
        this->txn.addReadSet(key, ts);
        this->readSet.insert(std::make_pair(key, std::make_tuple(value, ts)));
        //}
        gcb(status, key, value, ts);
    };
    txnclient->Get(tid, key, bufferCb, gtcb, timeout);
}

void BufferClient::Get(const std::string &key, const Timestamp &ts,
                       get_callback gcb, get_timeout_callback gtcb,
                       uint32_t timeout)
{
    // Read your own writes, check the write set first.
    if (txn.getWriteSet().find(key) != txn.getWriteSet().end())
    {
        gcb(REPLY_OK, key, (txn.getWriteSet().find(key))->second, Timestamp());
        return;
    }

    // Consistent reads, check the read set.
    if (txn.getReadSet().find(key) != txn.getReadSet().end())
    {
        auto readSetItr = readSet.find(key);
        ASSERT(readSetItr != readSet.end());
        gcb(REPLY_OK, key, std::get<0>(readSetItr->second),
            std::get<1>(readSetItr->second));
        return;
    }

    get_callback bufferCb = [this, gcb](int status, const std::string &key,
                                        const std::string &value,
                                        Timestamp ts)
    {
        // TODO: we still need to add a "failed" read to the read set
        //   (where failed ==> successful rpc, but no value exists for key)
        // if (status == REPLY_OK) {
        Debug("Added %lu.%lu to read set.", ts.getTimestamp(), ts.getID());
        this->txn.addReadSet(key, ts);
        this->readSet.insert(std::make_pair(key, std::make_tuple(value, ts)));
        //}
        gcb(status, key, value, ts);
    };
    txnclient->Get(tid, key, ts, bufferCb, gtcb, timeout);
}

void BufferClient::GetForUpdate(const std::string &key, const Timestamp &ts,
                                get_callback gcb, get_timeout_callback gtcb,
                                uint32_t timeout)
{
    // Read your own writes, check the write set first.
    if (txn.getWriteSet().find(key) != txn.getWriteSet().end())
    {
        gcb(REPLY_OK, key, (txn.getWriteSet().find(key))->second, Timestamp());
        return;
    }

    // Consistent reads, check the read set.
    if (txn.getReadSet().find(key) != txn.getReadSet().end())
    {
        auto readSetItr = readSet.find(key);
        ASSERT(readSetItr != readSet.end());
        gcb(REPLY_OK, key, std::get<0>(readSetItr->second),
            std::get<1>(readSetItr->second));
        return;
    }

    get_callback bufferCb = [this, gcb](int status, const std::string &key,
                                        const std::string &value,
                                        Timestamp ts)
    {
        // TODO: we still need to add a "failed" read to the read set
        //   (where failed ==> successful rpc, but no value exists for key)
        // if (status == REPLY_OK) {
        Debug("Added %lu.%lu to read set.", ts.getTimestamp(), ts.getID());
        this->txn.addReadSet(key, ts);
        this->readSet.insert(std::make_pair(key, std::make_tuple(value, ts)));
        //}
        gcb(status, key, value, ts);
    };
    txnclient->GetForUpdate(tid, key, ts, bufferCb, gtcb, timeout);
}

void BufferClient::Put(const std::string &key, const std::string &value,
                       put_callback pcb, put_timeout_callback ptcb,
                       uint32_t timeout)
{
    txn.addWriteSet(key, value);
    if (bufferPuts)
    {
        pcb(REPLY_OK, key, value);
    }
    else
    {
        txnclient->Put(tid, key, value, pcb, ptcb, timeout);
    }
}

void BufferClient::Put(const std::string &key, const std::string &value,
                       const Timestamp &ts, put_callback pcb,
                       put_timeout_callback ptcb, uint32_t timeout)
{
    txn.addWriteSet(key, value);
    if (bufferPuts)
    {
        pcb(REPLY_OK, key, value);
    }
    else
    {
        txnclient->Put(tid, key, value, ts, pcb, ptcb, timeout);
    }
}

void BufferClient::Prepare(const Timestamp &timestamp, prepare_callback pcb,
                           prepare_timeout_callback ptcb, uint32_t timeout)
{
    txnclient->Prepare(tid, txn, timestamp, pcb, ptcb, timeout);
}

void BufferClient::Commit(const Timestamp &ts, commit_callback ccb,
                          commit_timeout_callback ctcb, uint32_t timeout)
{
    txnclient->Commit(tid, txn, ts, ccb, ctcb, timeout);
}

void BufferClient::Abort(abort_callback acb, abort_timeout_callback atcb,
                         uint32_t timeout)
{
    txnclient->Abort(tid, txn, acb, atcb, timeout);
}
