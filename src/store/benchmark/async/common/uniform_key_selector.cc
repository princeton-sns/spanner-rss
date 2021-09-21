#include "store/benchmark/async/common/uniform_key_selector.h"

#include <cstdlib>

UniformKeySelector::UniformKeySelector(const std::vector<std::string> &keys)
    : KeySelector(keys) {
}

UniformKeySelector::~UniformKeySelector() {
}

int UniformKeySelector::GetKey(std::mt19937 &rand) {
  return rand() % GetNumKeys();
}
