#include "store/benchmark/async/retwis/follow.h"

namespace retwis {

Follow::Follow(KeySelector *keySelector, std::mt19937 &rand)
    : RetwisTransaction(keySelector, 2, rand, "follow") {}

Follow::~Follow() {
}

Operation Follow::GetNextOperation(std::size_t op_index) {
    Debug("FOLLOW %lu", op_index);
    if (op_index == 0) {
        return BeginRW();
    } else if (op_index == 1) {
        return GetForUpdate(GetKey(0));
    } else if (op_index == 2) {
        return Put(GetKey(0), GetKey(0));
    } else if (op_index == 3) {
        return GetForUpdate(GetKey(1));
    } else if (op_index == 4) {
        return Put(GetKey(1), GetKey(1));
    } else if (op_index == 5) {
        return Commit();
    } else {
        return Wait();
    }
}

}  // namespace retwis
