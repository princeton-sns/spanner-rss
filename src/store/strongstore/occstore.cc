// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/strongstore/occstore.cc:
 *   Key-value store with support for strong consistency using OCC
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

#include "store/strongstore/occstore.h"

#include "store/common/common.h"

namespace strongstore
{

    OCCStore::OCCStore() {}

    OCCStore::~OCCStore() {}

    int OCCStore::Get(uint64_t id, const std::string &key,
                      std::pair<Timestamp, std::string> &value)
    {
        int status;
        if (store.Get(key, value))
        {
            status = REPLY_OK;
        }
        else
        {
            status = REPLY_FAIL;
        }

        Debug("[%lu] GET for key %s; return ts %lu.%lu.", id,
              BytesToHex(key, 16).c_str(), value.first.getTimestamp(),
              value.first.getID());

        return status;
    }

    int OCCStore::Prepare(uint64_t id, const Transaction &txn,
                          std::unordered_map<uint64_t, int> &statuses)
    {
        Debug("[%lu] PREPARE.", id);

        if (prepared.find(id) != prepared.end())
        {
            Debug("[%lu] Already prepared!", id);
            return REPLY_OK;
        }

        // Do OCC checks.

        // Check for conflicts with the read set.
        for (auto &read : txn.getReadSet())
        {
            std::pair<Timestamp, std::string> cur;
            bool ret = store.Get(read.first, cur);
            Debug("[%lu] Read set contains key %s with version %lu.", id,
                  BytesToHex(read.first, 16).c_str(), read.second.getID());

            // TODO: this assert doesn't seem necessary. either the client should
            // not
            //   add a read to the read set when it fails to read a value or the
            //   client/server needs to agree on a way to distinguish between a
            //   successful read of an initial value and a failed read.
            // UW_ASSERT(ret);

            if (ret)
            {
                Debug("[%lu] Last committed write for key %s from txn %lu.", id,
                      BytesToHex(read.first, 16).c_str(), cur.first.getID());

                // If this key has been written since we read it, abort.
                if (cur.first != read.second)
                {
                    Debug(
                        "[%lu] ABORT wr conflict w/ committed key %s from txn %lu.",
                        id, BytesToHex(read.first, 16).c_str(), cur.first.getID());

                    stats.Increment("abort_committed_wr_conflict");
                    Abort(id, statuses);
                    return REPLY_FAIL;
                }
            }

            // If there is a pending write for this key, abort.
            if (pWrites.find(read.first) != pWrites.end())
            {
                Debug("[%lu] ABORT wr conflict w/ prepared key %s from txn %lu.",
                      id, BytesToHex(read.first, 16).c_str(),
                      *pWrites.find(read.first)->second.begin());
                stats.Increment("abort_prepared_wr_conflict");
                Abort(id, statuses);
                return REPLY_FAIL;
            }
        }

        // Check for conflicts with the write set.
        for (auto &write : txn.getWriteSet())
        {
            // If there is a pending read or write for this key, abort.
            Debug("[%lu] Write set contains key %s.", id,
                  BytesToHex(write.first, 16).c_str());

            if (pRW.find(write.first) != pRW.end())
            {
                Debug("[%lu] ABORT rw/ww conflict w/ prepared key %s from txn %lu.",
                      id, BytesToHex(write.first, 16).c_str(),
                      *pRW.find(write.first)->second.begin());
                stats.Increment("abort_prepared_*w_conflict");
                Abort(id, statuses);
                return REPLY_FAIL;
            }
        }

        // Otherwise, prepare this transaction for commit
        PrepareTransaction(id, txn);

        Debug("[%lu] PREPARED TO COMMIT.", id);
        return REPLY_OK;
    }

    bool OCCStore::Commit(uint64_t id, const Timestamp &ts,
                          std::unordered_map<uint64_t, int> &statuses)
    {
        Debug("[%lu] COMMIT w/ ts %lu.%lu.", id, ts.getTimestamp(), ts.getID());

        ASSERT(prepared.find(id) != prepared.end());

        Transaction txn = prepared[id];

        for (auto &write : txn.getWriteSet())
        {
            store.Put(write.first, std::make_pair(ts, write.second));
        }

        Clean(id);

        return !txn.getWriteSet().empty();
    }

    void OCCStore::Abort(uint64_t id, std::unordered_map<uint64_t, int> &statuses)
    {
        Debug("[%lu] ABORT.", id);

        Clean(id);
    }

    void OCCStore::Load(const std::string &key, const std::string &value,
                        const Timestamp &timestamp)
    {
        store.Put(key, std::make_pair(timestamp, value));
    }

    void OCCStore::PrepareTransaction(uint64_t id, const Transaction &txn)
    {
        prepared.insert(std::make_pair(id, txn));
        for (auto &write : txn.getWriteSet())
        {
            pWrites[write.first].insert(id);
            pRW[write.first].insert(id);
        }
        for (auto &read : txn.getReadSet())
        {
            pRW[read.first].insert(id);
        }
    }

    void OCCStore::Clean(uint64_t id)
    {
        auto preparedItr = prepared.find(id);
        if (preparedItr != prepared.end())
        {
            for (const auto &write : preparedItr->second.getWriteSet())
            {
                auto preparedWriteItr = pWrites.find(write.first);
                if (preparedWriteItr != pWrites.end())
                {
                    preparedWriteItr->second.erase(id);
                    if (preparedWriteItr->second.size() == 0)
                    {
                        pWrites.erase(preparedWriteItr);
                    }
                }
                auto preparedReadWriteItr = pRW.find(write.first);
                if (preparedReadWriteItr != pRW.end())
                {
                    preparedReadWriteItr->second.erase(id);
                    if (preparedReadWriteItr->second.size() == 0)
                    {
                        pRW.erase(preparedReadWriteItr);
                    }
                }
            }
            for (const auto &read : preparedItr->second.getReadSet())
            {
                auto preparedReadItr = pRW.find(read.first);
                if (preparedReadItr != pRW.end())
                {
                    preparedReadItr->second.erase(id);
                    if (preparedReadItr->second.size() == 0)
                    {
                        pRW.erase(preparedReadItr);
                    }
                }
            }
            prepared.erase(preparedItr);
        }
    }

} // namespace strongstore
