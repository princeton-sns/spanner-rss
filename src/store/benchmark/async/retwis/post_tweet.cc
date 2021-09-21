#include "store/benchmark/async/retwis/post_tweet.h"

namespace retwis {

PostTweet::PostTweet(KeySelector *keySelector, std::mt19937 &rand)
    : RetwisTransaction(keySelector, 5, rand, "post_tweet") {}

PostTweet::~PostTweet() {
}

Operation PostTweet::GetNextOperation(std::size_t op_index) {
    Debug("POST_TWEET %lu", op_index);
    if (op_index == 0) {
        return BeginRW();
    } else if (op_index < 7) {
        int k = (op_index - 1) / 2;
        if (op_index % 2 == 0) {
            return Put(GetKey(k), GetKey(k));
        } else {
            return GetForUpdate(GetKey(k));
        }
    } else if (op_index == 7) {
        return Put(GetKey(3), GetKey(3));
    } else if (op_index == 8) {
        return Put(GetKey(4), GetKey(4));
    } else if (op_index == 9) {
        return Commit();
    } else {
        return Wait();
    }
}

}  // namespace retwis
