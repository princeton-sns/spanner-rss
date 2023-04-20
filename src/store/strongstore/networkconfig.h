/***********************************************************************
 *
 * store/strongstore/networkconfig.h:
 *
 * Copyright 2022 Jeffrey Helt, Matthew Burke, Amit Levy, Wyatt Lloyd
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/
#ifndef _STRONG_NETWORKCONFIG_H_
#define _STRONG_NETWORKCONFIG_H_

#include <istream>
#include <nlohmann/json.hpp>
#include <string>

#include "lib/transport.h"

namespace strongstore
{

    class NetworkConfiguration
    {
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

} // namespace strongstore

#endif /* _STRONG_NETWORKCONFIG_H_ */
