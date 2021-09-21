#ifndef IO_UTILS_H
#define IO_UTILS_H

#include <iostream>
#include <string>

int ReadBytesFromStream(std::istream *is, std::string &value);
int WriteBytesToStream(std::ostream *os, const std::string &value);

#endif /* IO_UTILS_H */
