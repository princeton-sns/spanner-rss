#include "store/benchmark/async/retwis/get_timeline.h"

#include <unordered_set>

namespace retwis {

GetTimeline::GetTimeline(KeySelector *keySelector, std::mt19937 &rand)
    : RetwisTransaction(keySelector, 1 + rand() % 10, rand, "get_timeline") {}

GetTimeline::~GetTimeline() {
}

Operation GetTimeline::GetNextOperation(std::size_t op_index) {
    Debug("GET_TIMELINE %lu %lu", GetNumKeys(), op_index);

    if (op_index == 0) {
        return BeginRO();
    } else if (op_index == 1) {
        std::unordered_set<std::string> keys;
        for (std::size_t i = 0; i < GetNumKeys(); i++) {
            keys.insert(GetKey(i));
        }

        return ROCommit(std::move(keys));
    } else {
        return Wait();
    }
}

}  // namespace retwis
