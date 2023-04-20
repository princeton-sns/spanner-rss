/***********************************************************************
 *
 * store/strongstore/preparedtransaction.cc:
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
#include "store/strongstore/preparedtransaction.h"

namespace strongstore
{

    Value::Value(uint64_t transaction_id, const Timestamp &ts,
                 const std::string &key, const std::string &val)
        : transaction_id_{transaction_id}, ts_{ts}, key_{key}, val_{val} {}

    Value::Value(const proto::ReadReply &msg)
        : transaction_id_{msg.transaction_id()}, ts_{msg.timestamp()}, key_{msg.key()}, val_{msg.val()} {}

    Value::~Value() {}

    PreparedTransaction::PreparedTransaction(uint64_t transaction_id, const Timestamp &prepare_ts)
        : transaction_id_{transaction_id}, prepare_ts_{prepare_ts}, write_set_{} {}

    PreparedTransaction::PreparedTransaction(const proto::PreparedTransactionMessage &msg)
        : transaction_id_{msg.transaction_id()}, prepare_ts_{msg.prepare_timestamp()}
    {
        for (auto &w : msg.write_set())
        {
            write_set_.emplace(w.key(), w.value());
        }
    }

    PreparedTransaction::~PreparedTransaction() {}

    void PreparedTransaction::serialize(proto::PreparedTransactionMessage *msg) const
    {
        msg->set_transaction_id(transaction_id_);
        prepare_ts_.serialize(msg->mutable_prepare_timestamp());

        for (auto write : write_set_)
        {
            WriteMessage *writeMsg = msg->add_write_set();
            writeMsg->set_key(write.first);
            writeMsg->set_value(write.second);
        }
    }

}; // namespace strongstore