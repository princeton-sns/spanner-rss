#ifndef _ASYNC_TRANSACTION_H_
#define _ASYNC_TRANSACTION_H_

#include <functional>
#include <map>
#include <string>

#include "store/common/frontend/client.h"
#include "store/common/frontend/transaction_utils.h"

struct StringPointerComp {
    bool operator()(const std::string *a, const std::string *b) const {
        return *a < *b;
    }
};

typedef std::map<const std::string *, const std::string *, StringPointerComp>
    ReadValueMap;

class AsyncTransaction {
   public:
    AsyncTransaction() {}
    virtual ~AsyncTransaction() {}

    virtual Operation GetNextOperation(std::size_t op_index) = 0;

    virtual const std::string &GetTransactionType() = 0;
};

#endif
