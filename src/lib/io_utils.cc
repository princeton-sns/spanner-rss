#include "lib/io_utils.h"

#include <cmath>

int ReadBytesFromStream(std::istream *is, std::string &value) {
  char buffer[1024];
  uint32_t bytes;
  is->read(reinterpret_cast<char*>(&bytes), sizeof(uint32_t));
  if (*is) {
    while (!is->eof() && bytes > 0) {
      uint32_t amount = std::min(bytes, static_cast<uint32_t>(sizeof(buffer))); 
      is->read(buffer, amount);
      value.append(buffer, amount);
      bytes -= amount;
    }
  }
  return *is ? 0 : -1;
}

int WriteBytesToStream(std::ostream *os, const std::string &value) {
  uint32_t length = value.length();
  os->write(reinterpret_cast<char*>(&length), sizeof(uint32_t));
  os->write(value.c_str(), value.length());
  return sizeof(uint32_t) + value.length();
}

