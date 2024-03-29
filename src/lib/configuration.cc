// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * configuration.cc:
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

#include "lib/configuration.h"

#include <string.h>

#include <fstream>
#include <iostream>
#include <string>

#include "lib/assert.h"
#include "lib/message.h"

namespace transport
{

    ReplicaAddress::ReplicaAddress(const string &host, const string &port)
        : host(host), port(port) {}

    bool ReplicaAddress::operator==(const ReplicaAddress &other) const
    {
        return ((host == other.host) && (port == other.port));
    }

    Configuration::Configuration(const Configuration &c)
        : g(c.g),
          n(c.n),
          f(c.f),
          replicas(c.replicas),
          replicaHosts(c.replicaHosts),
          hosts(c.hosts),
          hasMulticast(c.hasMulticast),
          hasFC(c.hasFC),
          interfaces(c.interfaces)
    {
        multicastAddress = NULL;
        if (hasMulticast)
        {
            multicastAddress = new ReplicaAddress(*c.multicastAddress);
        }
        fcAddress = NULL;
        if (hasFC)
        {
            fcAddress = new ReplicaAddress(*c.fcAddress);
        }
    }

    Configuration::Configuration(
        int g, int n, int f, std::map<int, std::vector<ReplicaAddress>> replicas,
        ReplicaAddress *multicastAddress, ReplicaAddress *fcAddress,
        std::map<int, std::vector<std::string>> interfaces)
        : g(g), n(n), f(f), replicas(replicas), interfaces(interfaces)
    {
        if (multicastAddress)
        {
            hasMulticast = true;
            this->multicastAddress = new ReplicaAddress(*multicastAddress);
        }
        else
        {
            hasMulticast = false;
            multicastAddress = NULL;
        }

        if (fcAddress)
        {
            hasFC = true;
            this->fcAddress = new ReplicaAddress(*fcAddress);
        }
        else
        {
            hasFC = false;
            fcAddress = NULL;
        }

        for (const auto &r : replicas)
        {
            for (size_t idx = 0; idx < r.second.size(); ++idx)
            {
                if (hosts[r.first].find(r.second[idx].host) ==
                    hosts[r.first].end())
                {
                    hosts[r.first][r.second[idx].host] = hosts[r.first].size();
                    replicaHosts[r.first][idx] = hosts[r.first][r.second[idx].host];
                    hostToGroups[r.second[idx].host].insert(r.first);
                }
            }
        }
    }

    Configuration::Configuration(std::istream &file)
    {
        f = -1;
        hasMulticast = false;
        multicastAddress = NULL;
        hasFC = false;
        fcAddress = NULL;
        int group = -1;

        while (!file.eof())
        {
            // Read a line
            string line;
            getline(file, line);
            ;

            // Ignore comments
            if ((line.size() == 0) || (line[0] == '#'))
            {
                continue;
            }

            // Get the command
            // This is pretty horrible, but C++ does promise that &line[0]
            // is going to be a mutable contiguous buffer...
            char *cmd = strtok(&line[0], " \t");

            if (strcasecmp(cmd, "f") == 0)
            {
                char *arg = strtok(NULL, " \t");
                if (!arg)
                {
                    Panic("'f' configuration line requires an argument");
                }
                char *strtolPtr;
                f = strtoul(arg, &strtolPtr, 0);
                if ((*arg == '\0') || (*strtolPtr != '\0'))
                {
                    Panic("Invalid argument to 'f' configuration line");
                }
            }
            else if (strcasecmp(cmd, "group") == 0)
            {
                group++;
            }
            else if (strcasecmp(cmd, "replica") == 0)
            {
                if (group < 0)
                {
                    group = 0;
                }

                char *arg = strtok(NULL, " \t");
                if (!arg)
                {
                    Panic("'replica' configuration line requires an argument");
                }

                char *host = strtok(arg, ":");
                char *port = strtok(NULL, ":");
                char *interface = strtok(NULL, "");

                if (!host || !port)
                {
                    Panic("Configuration line format: 'replica group host:port'");
                }

                replicas[group].push_back(
                    ReplicaAddress(string(host), string(port)));
                if (interface != nullptr)
                {
                    interfaces[group].push_back(string(interface));
                }
                else
                {
                    interfaces[group].push_back(string());
                }
            }
            else if (strcasecmp(cmd, "multicast") == 0)
            {
                char *arg = strtok(NULL, " \t");
                if (!arg)
                {
                    Panic("'multicast' configuration line requires an argument");
                }

                char *host = strtok(arg, ":");
                char *port = strtok(NULL, "");

                if (!host || !port)
                {
                    Panic("Configuration line format: 'multicast host:port'");
                }

                multicastAddress = new ReplicaAddress(string(host), string(port));
                hasMulticast = true;
            }
            else if (strcasecmp(cmd, "fc") == 0)
            {
                char *arg = strtok(NULL, " \t");
                if (!arg)
                {
                    Panic("'fc' configuration line requires an argument");
                }

                char *host = strtok(arg, ":");
                char *port = strtok(NULL, "");

                if (!host || !port)
                {
                    Panic("Configuration line format: 'fc host:port'");
                }

                fcAddress = new ReplicaAddress(string(host), string(port));
                hasFC = true;
            }
            else
            {
                Panic("Unknown configuration directive: %s", cmd);
            }
        }

        g = replicas.size();

        if (g == 0)
        {
            Panic("Configuration did not specify any groups");
        }

        n = replicas[0].size();

        for (auto &kv : replicas)
        {
            if (kv.second.size() != (size_t)n)
            {
                Panic("All groups must contain the same number of replicas.");
            }
        }

        if (n == 0)
        {
            Panic("Configuration did not specify any replicas");
        }

        if (f == -1)
        {
            Panic("Configuration did not specify a 'f' parameter");
        }

        for (const auto &r : replicas)
        {
            for (size_t idx = 0; idx < r.second.size(); ++idx)
            {
                if (hosts[r.first].find(r.second[idx].host) ==
                    hosts[r.first].end())
                {
                    hosts[r.first][r.second[idx].host] = hosts[r.first].size();
                    replicaHosts[r.first][idx] = hosts[r.first][r.second[idx].host];
                }
            }
        }
    }

    Configuration::~Configuration()
    {
        if (hasMulticast)
        {
            delete multicastAddress;
        }
        if (hasFC)
        {
            delete fcAddress;
        }
    }

    // ReplicaAddress Configuration::replica(int group, int idx) const {
    //     return replicas.at(group)[idx];
    // }

    const ReplicaAddress &Configuration::replica(int group, int idx) const
    {
        return replicas.at(group)[idx];
    }

    const ReplicaAddress *Configuration::multicast() const
    {
        if (hasMulticast)
        {
            return multicastAddress;
        }
        else
        {
            return nullptr;
        }
    }

    const ReplicaAddress *Configuration::fc() const
    {
        if (hasFC)
        {
            return fcAddress;
        }
        else
        {
            return nullptr;
        }
    }

    std::string Configuration::Interface(int group, int idx) const
    {
        return this->interfaces.at(group)[idx];
    }

    int Configuration::QuorumSize() const { return f + 1; }

    int Configuration::FastQuorumSize() const { return f + (f + 1) / 2 + 1; }

    int Configuration::replicaHost(int group, int idx) const
    {
        const auto itr = replicaHosts.find(group);
        if (itr != replicaHosts.end())
        {
            const auto jtr = itr->second.find(idx);
            if (jtr != itr->second.end())
            {
                return jtr->second;
            }
        }
        return -1;
    }

    bool Configuration::IsLowestGroupOnHost(int group, int idx) const
    {
        auto itr = hostToGroups.find(replica(group, idx).host);
        ASSERT(itr != hostToGroups.end());
        return *itr->second.begin() == group;
    }

    bool Configuration::operator==(const Configuration &other) const
    {
        if ((n != other.n) || (f != other.f) || (replicas != other.replicas) ||
            (hasMulticast != other.hasMulticast) || (hasFC != other.hasFC))
        {
            return false;
        }

        if (hasMulticast)
        {
            if (*multicastAddress != *other.multicastAddress)
            {
                return false;
            }
        }

        if (hasFC)
        {
            if (*fcAddress != *other.fcAddress)
            {
                return false;
            }
        }
        return true;
    }

} // namespace transport
