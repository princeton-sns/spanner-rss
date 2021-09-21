#ifndef TRANSACTION_UTILS_H
#define TRANSACTION_UTILS_H

#include <string>
#include <unordered_set>
#include <vector>

enum OperationType {
    BEGIN_RW = 0,
    BEGIN_RO,
    GET,
    GET_FOR_UPDATE,
    PUT,
    COMMIT,
    ABORT,
    WAIT,
    ROCOMMIT,
};

struct Operation {
    OperationType type;
    std::string key;
    std::string value;
    const std::unordered_set<std::string> keys;
};

Operation BeginRW();

Operation BeginRO();

Operation Wait();

Operation Get(const std::string &key);

Operation GetForUpdate(const std::string &key);

Operation Put(const std::string &key,
              const std::string &value);

Operation Commit();

Operation Abort();

Operation ROCommit(const std::unordered_set<std::string> &keys);

Operation ROCommit(const std::unordered_set<std::string> &&keys);

#endif
