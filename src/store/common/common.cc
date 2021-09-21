#include "store/common/common.h"

std::string BytesToHex(const std::string &bytes, size_t maxLength) {
  static const char digits[] = "0123456789abcdef";
  std::string hex;
  size_t length = (bytes.size() < maxLength) ? bytes.size() : maxLength;
  for (size_t i = 0; i < length; ++i) {
    hex.push_back(digits[static_cast<uint8_t>(bytes[i]) >> 4]);
    hex.push_back(digits[static_cast<uint8_t>(bytes[i]) & 0xF]);
  }
  return hex;
}
