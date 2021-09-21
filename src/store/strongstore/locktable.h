#ifndef _STRONG_LOCK_TABLE_H_
#define _STRONG_LOCK_TABLE_H_

#include <cstdint>
#include <string>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/transaction.h"
#include "store/strongstore/woundwait.h"

namespace strongstore {

enum LockStatus { ACQUIRED,
                  WAITING,
                  FAIL };

struct LockAcquireResult {
    LockStatus status;
    std::unordered_set<uint64_t> wound_rws;
};

struct LockReleaseResult {
    std::unordered_set<uint64_t> notify_rws;
};

class LockTable {
   public:
    LockTable();
    ~LockTable();

    bool HasReadLock(uint64_t transaction_id, const std::string &key);
    LockAcquireResult AcquireReadLock(uint64_t transaction_id,
                                      const Timestamp &ts,
                                      const std::string &key);
    LockAcquireResult AcquireReadWriteLock(uint64_t transaction_id,
                                           const Timestamp &ts,
                                           const std::string &key);

    LockAcquireResult AcquireLocks(uint64_t transaction_id,
                                   const Transaction &transaction);
    LockReleaseResult ReleaseLocks(uint64_t transaction_id,
                                   const Transaction &transaction);

   private:
    WoundWait locks_;

    LockStatus ConvertToResultStatus(int status);
};

}  // namespace strongstore

#endif /* _STRONG_LOCK_TABLE_H_ */
