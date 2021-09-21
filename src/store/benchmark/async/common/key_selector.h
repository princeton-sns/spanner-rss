#ifndef KEY_SELECTOR_H
#define KEY_SELECTOR_H

#include <random>
#include <string>
#include <vector>

class KeySelector {
 public:
  KeySelector(const std::vector<std::string> &keys);
  virtual ~KeySelector();

  virtual int GetKey(std::mt19937 &rand) = 0;

  inline const std::string &GetKey(int idx) const { return keys[idx]; }
  inline size_t GetNumKeys() const { return keys.size(); }

 private:
  const std::vector<std::string> &keys;

};

#endif /* KEY_SELECTOR_H */
