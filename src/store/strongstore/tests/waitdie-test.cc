#include "store/strongstore/waitdie.h"

#include <gtest/gtest.h>

#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "store/common/timestamp.h"

namespace strongstore {

TEST(WaitDie, BasicReadLock) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForRead("lock", 1, Timestamp());
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    wd.ReleaseForRead("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, BasicWriteLock) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForWrite("lock", 1, Timestamp());
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    wd.ReleaseForWrite("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, BasicReadWriteLock) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForRead("lock", 1, Timestamp());
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    status = wd.LockForWrite("lock", 1, Timestamp());
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ_WRITE);

    wd.ReleaseForWrite("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    wd.ReleaseForRead("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, MultiReadLock) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForRead("lock", 1, Timestamp());
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    status = wd.LockForRead("lock", 2, Timestamp());
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    status = wd.LockForRead("lock", 3, Timestamp());
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    wd.ReleaseForRead("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);
    wd.ReleaseForRead("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);
    wd.ReleaseForRead("lock", 3, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, MultiWriteLockWait) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForWrite("lock", 1, Timestamp(1));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    status = wd.LockForWrite("lock", 2, Timestamp(0));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    wd.ReleaseForWrite("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(notify_rws.size(), 1);
    ASSERT_EQ(notify_rws.count(2), 1);
    notify_rws.clear();

    wd.ReleaseForWrite("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, MultiWriteLockDie) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForWrite("lock", 1, Timestamp(0));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    status = wd.LockForWrite("lock", 2, Timestamp(1));
    ASSERT_EQ(status, REPLY_FAIL);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    wd.ReleaseForWrite("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, MultiReadWriteLockWait) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForRead("lock", 1, Timestamp(1));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    status = wd.LockForRead("lock", 2, Timestamp(0));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    status = wd.LockForWrite("lock", 2, Timestamp(0));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    wd.ReleaseForRead("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ_WRITE);

    ASSERT_EQ(notify_rws.size(), 1);
    ASSERT_EQ(notify_rws.count(2), 1);
    notify_rws.clear();

    wd.ReleaseForWrite("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);
    wd.ReleaseForRead("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, MultiReadWriteLockDie) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForRead("lock", 1, Timestamp(0));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    status = wd.LockForRead("lock", 2, Timestamp(1));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    status = wd.LockForWrite("lock", 2, Timestamp(1));
    ASSERT_EQ(status, REPLY_FAIL);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    wd.ReleaseForRead("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    ASSERT_EQ(notify_rws.size(), 0);
    notify_rws.clear();

    wd.ReleaseForWrite("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);
    wd.ReleaseForRead("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, MergeReadWriteWaiter) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForWrite("lock", 1, Timestamp(1));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    status = wd.LockForRead("lock", 2, Timestamp(0));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    status = wd.LockForWrite("lock", 2, Timestamp(0));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    wd.ReleaseForWrite("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ_WRITE);

    ASSERT_EQ(notify_rws.size(), 1);
    ASSERT_EQ(notify_rws.count(2), 1);
    notify_rws.clear();

    wd.ReleaseForWrite("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);
    wd.ReleaseForRead("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, MultiReadWaiter) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForWrite("lock", 1, Timestamp(3));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    status = wd.LockForRead("lock", 2, Timestamp(2));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    status = wd.LockForRead("lock", 3, Timestamp(1));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    wd.ReleaseForWrite("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    ASSERT_EQ(notify_rws.size(), 2);
    ASSERT_EQ(notify_rws.count(2), 1);
    ASSERT_EQ(notify_rws.count(3), 1);
    notify_rws.clear();

    wd.ReleaseForRead("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    ASSERT_EQ(notify_rws.size(), 0);

    wd.ReleaseForRead("lock", 3, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, MultiReadWaiterRelease1) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForWrite("lock", 1, Timestamp(3));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    status = wd.LockForRead("lock", 2, Timestamp(2));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    status = wd.LockForRead("lock", 3, Timestamp(1));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    wd.ReleaseForRead("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(notify_rws.size(), 0);

    wd.ReleaseForRead("lock", 3, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(notify_rws.size(), 0);

    wd.ReleaseForWrite("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, MultiReadWaiterRelease2) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForWrite("lock", 1, Timestamp(3));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    status = wd.LockForRead("lock", 2, Timestamp(2));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    status = wd.LockForRead("lock", 3, Timestamp(1));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    wd.ReleaseForRead("lock", 3, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(notify_rws.size(), 0);

    wd.ReleaseForRead("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(notify_rws.size(), 0);

    wd.ReleaseForWrite("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, MergeMultiReadWriteWaiter) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForWrite("lock", 1, Timestamp(3));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    status = wd.LockForRead("lock", 2, Timestamp(2));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    status = wd.LockForRead("lock", 3, Timestamp(1));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    status = wd.LockForWrite("lock", 3, Timestamp(1));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    wd.ReleaseForWrite("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    ASSERT_EQ(notify_rws.size(), 1);
    ASSERT_EQ(notify_rws.count(2), 1);
    notify_rws.clear();

    wd.ReleaseForRead("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ_WRITE);

    ASSERT_EQ(notify_rws.size(), 1);
    ASSERT_EQ(notify_rws.count(3), 1);
    notify_rws.clear();

    wd.ReleaseForRead("lock", 3, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);
    wd.ReleaseForWrite("lock", 3, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, MergeWriteReadWaiter) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForWrite("lock", 1, Timestamp(1));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    status = wd.LockForWrite("lock", 2, Timestamp(0));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    status = wd.LockForRead("lock", 2, Timestamp(0));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    wd.ReleaseForWrite("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ_WRITE);

    ASSERT_EQ(notify_rws.size(), 1);
    ASSERT_EQ(notify_rws.count(2), 1);
    notify_rws.clear();

    wd.ReleaseForRead("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);
    wd.ReleaseForWrite("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, ReleaseReadWaiter) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForWrite("lock", 1, Timestamp(1));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    status = wd.LockForRead("lock", 2, Timestamp(0));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    wd.ReleaseForRead("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(notify_rws.size(), 0);

    wd.ReleaseForWrite("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, ReleaseWriteWaiter) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForRead("lock", 1, Timestamp(1));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    status = wd.LockForWrite("lock", 2, Timestamp(0));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    wd.ReleaseForWrite("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    ASSERT_EQ(notify_rws.size(), 0);

    wd.ReleaseForRead("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, ReleaseMultiWriteWaiter) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForRead("lock", 1, Timestamp(2));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    status = wd.LockForWrite("lock", 2, Timestamp(1));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    status = wd.LockForWrite("lock", 3, Timestamp(0));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    wd.ReleaseForWrite("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    ASSERT_EQ(notify_rws.size(), 0);

    wd.ReleaseForRead("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(notify_rws.size(), 1);
    ASSERT_EQ(notify_rws.count(3), 1);
    notify_rws.clear();

    wd.ReleaseForWrite("lock", 3, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, ReleaseReadWriteWaiter1) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForWrite("lock", 1, Timestamp(1));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    status = wd.LockForRead("lock", 2, Timestamp(0));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    status = wd.LockForWrite("lock", 2, Timestamp(0));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    wd.ReleaseForRead("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);
    wd.ReleaseForWrite("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(notify_rws.size(), 0);

    wd.ReleaseForWrite("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, ReleaseReadWriteWaiter2) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForWrite("lock", 1, Timestamp(1));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    status = wd.LockForRead("lock", 2, Timestamp(0));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    status = wd.LockForWrite("lock", 2, Timestamp(0));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    wd.ReleaseForRead("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(notify_rws.size(), 0);

    wd.ReleaseForWrite("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(notify_rws.size(), 1);
    ASSERT_EQ(notify_rws.count(2), 1);
    notify_rws.clear();

    wd.ReleaseForWrite("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, ReleaseReadWriteWaiter3) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForWrite("lock", 1, Timestamp(1));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    status = wd.LockForRead("lock", 2, Timestamp(0));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    status = wd.LockForWrite("lock", 2, Timestamp(0));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    wd.ReleaseForWrite("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(notify_rws.size(), 0);

    wd.ReleaseForWrite("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    ASSERT_EQ(notify_rws.size(), 1);
    ASSERT_EQ(notify_rws.count(2), 1);
    notify_rws.clear();

    wd.ReleaseForRead("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, Release2WriteWaiter) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForRead("lock", 1, Timestamp(2));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    status = wd.LockForWrite("lock", 2, Timestamp(1));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    wd.ReleaseForWrite("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    ASSERT_EQ(notify_rws.size(), 0);

    status = wd.LockForWrite("lock", 3, Timestamp(0));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    wd.ReleaseForRead("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(notify_rws.size(), 1);
    ASSERT_EQ(notify_rws.count(3), 1);
    notify_rws.clear();

    wd.ReleaseForWrite("lock", 3, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, Release2WriteReadWaiter) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForRead("lock", 1, Timestamp(3));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    status = wd.LockForWrite("lock", 2, Timestamp(2));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    status = wd.LockForWrite("lock", 3, Timestamp(1));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    wd.ReleaseForWrite("lock", 3, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    ASSERT_EQ(notify_rws.size(), 0);

    status = wd.LockForRead("lock", 4, Timestamp(0));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    wd.ReleaseForRead("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(notify_rws.size(), 1);
    ASSERT_EQ(notify_rws.count(2), 1);
    notify_rws.clear();

    wd.ReleaseForWrite("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    ASSERT_EQ(notify_rws.size(), 1);
    ASSERT_EQ(notify_rws.count(4), 1);
    notify_rws.clear();

    wd.ReleaseForRead("lock", 4, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, ReleaseWriteReadWaiter) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForRead("lock", 1, Timestamp(2));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    status = wd.LockForWrite("lock", 2, Timestamp(1));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    wd.ReleaseForWrite("lock", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    ASSERT_EQ(notify_rws.size(), 0);

    status = wd.LockForRead("lock", 3, Timestamp(0));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    wd.ReleaseForRead("lock", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), LOCKED_FOR_READ);

    ASSERT_EQ(notify_rws.size(), 0);

    wd.ReleaseForRead("lock", 3, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, WaitTwoLocks) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForRead("lock1", 1, Timestamp(2));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock1"), LOCKED_FOR_READ);

    status = wd.LockForRead("lock2", 1, Timestamp(2));
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(wd.GetLockState("lock2"), LOCKED_FOR_READ);

    status = wd.LockForWrite("lock1", 2, Timestamp(1));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock1"), LOCKED_FOR_READ);

    status = wd.LockForWrite("lock2", 2, Timestamp(1));
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(wd.GetLockState("lock2"), LOCKED_FOR_READ);

    wd.ReleaseForRead("lock1", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock1"), LOCKED_FOR_WRITE);

    ASSERT_EQ(notify_rws.size(), 0);

    wd.ReleaseForRead("lock2", 1, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock2"), LOCKED_FOR_WRITE);

    ASSERT_EQ(notify_rws.size(), 1);
    ASSERT_EQ(notify_rws.count(2), 1);
    notify_rws.clear();

    wd.ReleaseForWrite("lock1", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock1"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);

    wd.ReleaseForWrite("lock2", 2, notify_rws);
    ASSERT_EQ(wd.GetLockState("lock2"), UNLOCKED);

    ASSERT_EQ(notify_rws.size(), 0);
}

};  // namespace strongstore
