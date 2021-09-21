#ifndef _STRONG_PREPARED_TRANSACTION_H_
#define _STRONG_PREPARED_TRANSACTION_H_

#include <algorithm>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>

#include "store/common/timestamp.h"
#include "store/strongstore/strong-proto.pb.h"

namespace strongstore {

class Value {
   public:
    Value(uint64_t transaction_id, const Timestamp &ts,
          const std::string &key, const std::string &val);
    Value(const proto::ReadReply &msg);
    ~Value();

    uint64_t
    transaction_id() const { return transaction_id_; }
    const Timestamp &ts() const { return ts_; }
    const std::string key() const { return key_; }
    const std::string val() const { return val_; }

   private:
    uint64_t transaction_id_;
    Timestamp ts_;
    std::string key_;
    std::string val_;
};

class PreparedTransaction {
   public:
    PreparedTransaction(uint64_t transaction_id, const Timestamp &prepare_ts);
    PreparedTransaction(const proto::PreparedTransactionMessage &msg);
    ~PreparedTransaction();

    void serialize(proto::PreparedTransactionMessage *msg) const;

    const Timestamp &prepare_ts() const { return prepare_ts_; }
    void update_prepare_ts(const Timestamp &prepare_ts) { prepare_ts_ = std::max(prepare_ts_, prepare_ts); }

    uint64_t transaction_id() const { return transaction_id_; }

    const std::unordered_map<std::string, std::string> &write_set() const { return write_set_; }
    void add_write_set(const std::unordered_map<std::string, std::string> &write_set) {
        write_set_.insert(write_set.begin(), write_set.end());
    }
    void add_write_set(const std::pair<std::string, std::string> &w) {
        write_set_.insert(w);
    }

   private:
    uint64_t transaction_id_;
    Timestamp prepare_ts_;
    std::unordered_map<std::string, std::string> write_set_;
};

};  // namespace strongstore

#endif /* _STRONG_PREPARED_TRANSACTION_H_ */