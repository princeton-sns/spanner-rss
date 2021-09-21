#include "store/benchmark/async/retwis/add_user.h"

namespace retwis {

AddUser::AddUser(KeySelector *keySelector, std::mt19937 &rand)
    : RetwisTransaction(keySelector, 4, rand, "add_user") {}

AddUser::~AddUser() {
}

Operation AddUser::GetNextOperation(std::size_t op_index) {
    Debug("ADD_USER %lu", op_index);
    if (op_index == 0) {
        return BeginRW();
    } else if (op_index == 1) {
        return GetForUpdate(GetKey(0));
    } else if (op_index < 5) {
        return Put(GetKey(op_index - 1), GetKey(op_index - 1));
    } else if (op_index == 5) {
        return Commit();
    } else {
        return Wait();
    }
}

}  // namespace retwis
