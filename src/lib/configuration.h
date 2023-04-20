// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * configuration.h:
 *   Representation of a replica group configuration, i.e. the number
 *   and list of replicas in the group
 *
 * Copyright 2022 Jeffrey Helt, Matthew Burke, Amit Levy, Wyatt Lloyd
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
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

#ifndef _LIB_CONFIGURATION_H_
#define _LIB_CONFIGURATION_H_

#include <stdbool.h>

#include <fstream>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "lib/viewstamp.h"

using std::string;

namespace transport
{

    struct ReplicaAddress
    {
        string host;
        string port;
        ReplicaAddress(const string &host, const string &port);
        bool operator==(const ReplicaAddress &other) const;
        inline bool operator!=(const ReplicaAddress &other) const
        {
            return !(*this == other);
        }
    };

    class Configuration
    {
    public:
        Configuration(const Configuration &c);
        Configuration(int g, int n, int f,
                      std::map<int, std::vector<ReplicaAddress>> replicas,
                      ReplicaAddress *multicastAddress = nullptr,
                      ReplicaAddress *fcAddress = nullptr,
                      std::map<int, std::vector<std::string>> interfaces =
                          std::map<int, std::vector<std::string>>());
        Configuration(std::istream &file);
        virtual ~Configuration();
        // ReplicaAddress replica(int group, int idx) const;
        const ReplicaAddress &replica(int group, int idx) const;
        const ReplicaAddress *multicast() const;
        const ReplicaAddress *fc() const;
        std::string Interface(int group, int idx) const;
        inline int GetLeaderIndex(view_t view) const { return (view % n); };
        int QuorumSize() const;
        int FastQuorumSize() const;
        bool operator==(const Configuration &other) const;
        inline bool operator!=(const Configuration &other) const
        {
            return !(*this == other);
        }
        int replicaHost(int group, int idx) const;
        bool IsLowestGroupOnHost(int group, int idx) const;

    public:
        int g; // number of groups
        int n; // number of replicas per group
        int f; // number of failures tolerated (assume homogeneous across groups)
    private:
        std::map<int, std::vector<ReplicaAddress>> replicas;
        std::map<int, std::map<int, int>> replicaHosts;
        std::map<int, std::map<std::string, int>> hosts;
        std::map<std::string, std::set<int>> hostToGroups;
        ReplicaAddress *multicastAddress;
        bool hasMulticast;
        ReplicaAddress *fcAddress;
        bool hasFC;
        std::map<int, std::vector<std::string>> interfaces;
    };

} // namespace transport

namespace std
{
    template <>
    struct hash<transport::ReplicaAddress>
    {
        size_t operator()(const transport::ReplicaAddress &x) const
        {
            return hash<string>()(x.host) * 37 + hash<string>()(x.port);
        }
    };
} // namespace std

namespace std
{
    template <>
    struct hash<transport::Configuration>
    {
        size_t operator()(const transport::Configuration &x) const
        {
            size_t out = 0;
            out = x.n * 37 + x.f;
            for (int i = 0; i < x.g; i++)
            {
                for (int j = 0; j < x.n; j++)
                {
                    out *= 37;
                    out += hash<transport::ReplicaAddress>()(x.replica(i, j));
                }
            }
            return out;
        }
    };

} // namespace std

#endif /* _LIB_CONFIGURATION_H_ */
