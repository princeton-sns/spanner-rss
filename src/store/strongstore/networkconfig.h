#ifndef _STRONG_NETWORKCONFIG_H_
#define _STRONG_NETWORKCONFIG_H_

#include <istream>
#include <nlohmann/json.hpp>
#include <string>

#include "lib/transport.h"

namespace strongstore {

class NetworkConfiguration {
   public:
    const std::string INVALID_REGION = "invalid";

    NetworkConfiguration(transport::Configuration &tport_config,
                         std::istream &file);
    ~NetworkConfiguration();

    const std::string &GetRegion(const std::string &host) const;
    const std::string &GetRegion(int shard_idx, int replica_idx) const;

    uint16_t GetOneWayLatency(const std::string &src_region,
                              const std::string &dst_region) const;

    uint16_t GetMinQuorumLatency(int shard_idx, int leader_idx) const;

   private:
    nlohmann::json net_config_json_;
    transport::Configuration &tport_config_;
};

}  // namespace strongstore

#endif /* _STRONG_NETWORKCONFIG_H_ */
