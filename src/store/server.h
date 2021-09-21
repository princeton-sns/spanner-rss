#ifndef STORE_SERVER_H
#define STORE_SERVER_H

#include "store/common/timestamp.h"
#include "store/common/stats.h"

#include <string>

class Server {
 public:
  Server() { }
  virtual ~Server() { }
  virtual void Load(const std::string &key, const std::string &value,
      const Timestamp timestamp) = 0;

  virtual Stats &GetStats() = 0;
};

#endif /* STORE_SERVER_H */
