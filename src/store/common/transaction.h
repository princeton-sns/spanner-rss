// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * common/transaction.h:
 *   A Transaction representation.
 *
 **********************************************************************/

#ifndef _TRANSACTION_H_
#define _TRANSACTION_H_

#include <unordered_map>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/common-proto.pb.h"
#include "store/common/timestamp.h"

// Reply types
#define REPLY_OK 0
#define REPLY_FAIL 1
#define REPLY_RETRY 2
#define REPLY_ABSTAIN 3
#define REPLY_TIMEOUT 4
#define REPLY_NETWORK_FAILURE 5
#define REPLY_WAIT 6
#define REPLY_PREPARED 7
#define REPLY_MAX 8

class Transaction {
   private:
    // map between key and timestamp at
    // which the read happened and how
    // many times this key has been read
    std::unordered_map<std::string, Timestamp> readSet;

    // map between key and value(s)
    std::unordered_map<std::string, std::string> writeSet;

    // Start time (used for deadlock prevention)
    Timestamp start_time_;

   public:
    Transaction();
    Transaction(const TransactionMessage &msg);
    ~Transaction();

    const Timestamp &start_time() const;
    const std::unordered_map<std::string, Timestamp> &getReadSet() const;
    const std::unordered_map<std::string, std::string> &getWriteSet() const;
    std::unordered_map<std::string, std::string> &getWriteSet();
    void serialize(TransactionMessage *msg) const;

    void addReadSet(const std::string &key, const Timestamp &readTime);
    void addWriteSet(const std::string &key, const std::string &value);
    void set_start_time(const Timestamp &ts);

    void add_read_write_sets(const Transaction &other);
};

#endif /* _TRANSACTION_H_ */
