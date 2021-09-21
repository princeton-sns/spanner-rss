#ifndef RETWIS_GET_TIMELINE_H
#define RETWIS_GET_TIMELINE_H

#include <functional>

#include "store/benchmark/async/retwis/retwis_transaction.h"

namespace retwis {

class GetTimeline : public RetwisTransaction {
   public:
    GetTimeline(KeySelector *keySelector, std::mt19937 &rand);
    virtual ~GetTimeline();

   protected:
    Operation GetNextOperation(std::size_t op_index) override;
};

}  // namespace retwis

#endif /* RETWIS_GET_TIMELINE_H */
