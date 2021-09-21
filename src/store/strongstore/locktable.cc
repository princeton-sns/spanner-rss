#include "store/strongstore/locktable.h"

namespace strongstore {

LockTable::LockTable() {}

LockTable::~LockTable() {}

LockStatus LockTable::ConvertToResultStatus(int status) {
    if (status == REPLY_OK) {
        return ACQUIRED;
    } else if (status == REPLY_WAIT) {
        return WAITING;
    } else if (status == REPLY_FAIL) {
        return FAIL;
    } else {
        NOT_REACHABLE();
    }
}

LockAcquireResult LockTable::AcquireReadLock(uint64_t transaction_id,
                                             const Timestamp &ts,
                                             const std::string &key) {
    LockAcquireResult r;
    int status = locks_.LockForRead(key, transaction_id, ts, r.wound_rws);
    Debug("[%lu] LockForRead returned status %d", transaction_id, status);
    r.status = ConvertToResultStatus(status);
    return r;
}

bool LockTable::HasReadLock(uint64_t transaction_id, const std::string &key) {
    return locks_.HasReadLock(key, transaction_id);
}

LockAcquireResult LockTable::AcquireReadWriteLock(uint64_t transaction_id,
                                                  const Timestamp &ts,
                                                  const std::string &key) {
    LockAcquireResult r;

    int status = locks_.LockForWrite(key, transaction_id, ts, r.wound_rws);
    Debug("[%lu] LockForWrite returned status %d", transaction_id, status);
    int status2 = locks_.LockForRead(key, transaction_id, ts, r.wound_rws);
    Debug("[%lu] LockForRead returned status %d", transaction_id, status2);
    ASSERT(status == status2);

    r.status = ConvertToResultStatus(status);
    return r;
}

LockAcquireResult LockTable::AcquireLocks(uint64_t transaction_id,
                                          const Transaction &transaction) {
    const Timestamp &start_ts = transaction.start_time();
    // Debug("[%lu] start_time: %lu.%lu", transaction_id,
    // start_ts.getTimestamp(), start_ts.getID());

    LockAcquireResult r;
    int ret = REPLY_OK;

    // get read locks
    for (auto &read : transaction.getReadSet()) {
        int status = locks_.LockForRead(read.first, transaction_id, start_ts,
                                        r.wound_rws);
        Debug("[%lu] LockForRead returned status %d", transaction_id, status);
        if (ret == REPLY_OK && status == REPLY_WAIT) {
            ret = REPLY_WAIT;
        } else if (status == REPLY_FAIL) {
            ret = REPLY_FAIL;
        }
    }

    // get write locks
    for (auto &write : transaction.getWriteSet()) {
        int status = locks_.LockForWrite(write.first, transaction_id, start_ts,
                                         r.wound_rws);
        Debug("[%lu] LockForWrite returned status %d", transaction_id, status);
        if (ret == REPLY_OK && status == REPLY_WAIT) {
            ret = REPLY_WAIT;
        } else if (status == REPLY_FAIL) {
            ret = REPLY_FAIL;
        }
    }

    r.status = ConvertToResultStatus(ret);
    return r;
}

LockReleaseResult LockTable::ReleaseLocks(uint64_t transaction_id,
                                          const Transaction &transaction) {
    LockReleaseResult r;

    for (auto &write : transaction.getWriteSet()) {
        Debug("[%lu] ReleaseForWrite: %s", transaction_id, write.first.c_str());
        locks_.ReleaseForWrite(write.first, transaction_id, r.notify_rws);
    }

    for (auto &read : transaction.getReadSet()) {
        Debug("[%lu] ReleaseForRead: %s", transaction_id, read.first.c_str());
        locks_.ReleaseForRead(read.first, transaction_id, r.notify_rws);
    }

    return r;
}

}  // namespace strongstore
