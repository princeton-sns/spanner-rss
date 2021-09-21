// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * common/transaction.cc
 *   A transaction implementation.
 *
 **********************************************************************/

#include "store/common/transaction.h"

using namespace std;

Transaction::Transaction() : readSet{}, writeSet{}, start_time_{} {}

Transaction::Transaction(const TransactionMessage &msg)
    : start_time_{msg.starttime()} {
    for (int i = 0; i < msg.readset_size(); i++) {
        ReadMessage readMsg = msg.readset(i);
        readSet[readMsg.key()] = Timestamp(readMsg.readtime());
    }

    for (int i = 0; i < msg.writeset_size(); i++) {
        WriteMessage writeMsg = msg.writeset(i);
        writeSet[writeMsg.key()] = writeMsg.value();
    }
}

Transaction::~Transaction() {}

const unordered_map<string, Timestamp> &Transaction::getReadSet() const {
    return readSet;
}

const unordered_map<string, string> &Transaction::getWriteSet() const {
    return writeSet;
}

unordered_map<string, string> &Transaction::getWriteSet() {
    return writeSet;
}

const Timestamp &Transaction::start_time() const { return start_time_; }

void Transaction::set_start_time(const Timestamp &ts) { start_time_ = ts; }

void Transaction::addReadSet(const string &key, const Timestamp &readTime) {
    readSet[key] = readTime;
}

void Transaction::addWriteSet(const string &key, const string &value) {
    writeSet[key] = value;
}

void Transaction::add_read_write_sets(const Transaction &other) {
    for (auto &kt : other.getReadSet()) {
        readSet[kt.first] = kt.second;
    }

    for (auto &kv : other.getWriteSet()) {
        writeSet[kv.first] = kv.second;
    }
}

void Transaction::serialize(TransactionMessage *msg) const {
    start_time_.serialize(msg->mutable_starttime());
    for (auto read : readSet) {
        ReadMessage *readMsg = msg->add_readset();
        readMsg->set_key(read.first);
        read.second.serialize(readMsg->mutable_readtime());
    }

    for (auto write : writeSet) {
        WriteMessage *writeMsg = msg->add_writeset();
        writeMsg->set_key(write.first);
        writeMsg->set_value(write.second);
    }
}
