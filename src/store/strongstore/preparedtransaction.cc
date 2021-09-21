#include "store/strongstore/preparedtransaction.h"

namespace strongstore {

Value::Value(uint64_t transaction_id, const Timestamp &ts,
             const std::string &key, const std::string &val)
    : transaction_id_{transaction_id}, ts_{ts}, key_{key}, val_{val} {}

Value::Value(const proto::ReadReply &msg)
    : transaction_id_{msg.transaction_id()}, ts_{msg.timestamp()}, key_{msg.key()}, val_{msg.val()} {}

Value::~Value() {}

PreparedTransaction::PreparedTransaction(uint64_t transaction_id, const Timestamp &prepare_ts)
    : transaction_id_{transaction_id}, prepare_ts_{prepare_ts}, write_set_{} {}

PreparedTransaction::PreparedTransaction(const proto::PreparedTransactionMessage &msg)
    : transaction_id_{msg.transaction_id()}, prepare_ts_{msg.prepare_timestamp()} {
    for (auto &w : msg.write_set()) {
        write_set_.emplace(w.key(), w.value());
    }
}

PreparedTransaction::~PreparedTransaction() {}

void PreparedTransaction::serialize(proto::PreparedTransactionMessage *msg) const {
    msg->set_transaction_id(transaction_id_);
    prepare_ts_.serialize(msg->mutable_prepare_timestamp());

    for (auto write : write_set_) {
        WriteMessage *writeMsg = msg->add_write_set();
        writeMsg->set_key(write.first);
        writeMsg->set_value(write.second);
    }
}

};  // namespace strongstore