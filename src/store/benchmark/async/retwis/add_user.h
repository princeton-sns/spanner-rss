#ifndef RETWIS_ADD_USER_H
#define RETWIS_ADD_USER_H

#include <functional>

#include "store/benchmark/async/retwis/retwis_transaction.h"

namespace retwis {

class AddUser : public RetwisTransaction {
   public:
    AddUser(KeySelector *keySelector, std::mt19937 &rand);
    virtual ~AddUser();

   protected:
    Operation GetNextOperation(std::size_t op_index) override;
};

}  // namespace retwis

#endif /* RETWIS_ADD_USER_H */
