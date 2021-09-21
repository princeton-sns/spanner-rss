#ifndef UNIFORM_KEY_SELECTOR_H
#define UNIFORM_KEY_SELECTOR_H

#include "store/benchmark/async/common/key_selector.h"

class UniformKeySelector : public KeySelector {
 public:
  UniformKeySelector(const std::vector<std::string> &keys);
  virtual ~UniformKeySelector();

  virtual int GetKey(std::mt19937 &rand) override;

};

#endif /* UNIFORM_KEY_SELECTOR_H */
