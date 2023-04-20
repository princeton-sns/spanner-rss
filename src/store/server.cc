/***********************************************************************
 *
 * store/server.cc:
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
#include "store/server.h"

#include <gflags/gflags.h>
#include <valgrind/callgrind.h>

#include <csignal>

#include "lib/io_utils.h"
#include "lib/tcptransport.h"
#include "lib/transport.h"
#include "lib/udptransport.h"
#include "store/common/partitioner.h"
#include "store/strongstore/server.h"

enum protocol_t
{
    PROTO_UNKNOWN,
    PROTO_STRONG
};

enum transmode_t
{
    TRANS_UNKNOWN,
    TRANS_UDP,
    TRANS_TCP,
};

/**
 * System settings.
 */
DEFINE_uint64(server_id, 0, "unique identifier for server");
DEFINE_string(replica_config_path, "",
              "path to replication configuration file");
DEFINE_string(shard_config_path, "", "path to shard configuration file");
DEFINE_uint64(replica_idx, 0,
              "index of replica in replication configuration file");
DEFINE_uint64(group_idx, 0, "index of the shard to which this replica belongs");
DEFINE_uint64(num_shards, 1, "number of shards in the system");
DEFINE_bool(debug_stats, false, "record stats related to debugging");

const std::string protocol_args[] = {
    "strong",
};
const protocol_t protos[]{
    PROTO_STRONG,
};
static bool ValidateProtocol(const char *flagname, const std::string &value)
{
    int n = sizeof(protocol_args);
    for (int i = 0; i < n; ++i)
    {
        if (value == protocol_args[i])
        {
            return true;
        }
    }
    std::cerr << "Invalid value for --" << flagname << ": " << value
              << std::endl;
    return false;
}
DEFINE_string(protocol, protocol_args[0],
              "the protocol to use during this"
              " experiment");
DEFINE_validator(protocol, &ValidateProtocol);

const std::string trans_args[] = {"udp", "tcp"};

const transmode_t transmodes[]{TRANS_UDP, TRANS_TCP};
static bool ValidateTransMode(const char *flagname, const std::string &value)
{
    int n = sizeof(trans_args);
    for (int i = 0; i < n; ++i)
    {
        if (value == trans_args[i])
        {
            return true;
        }
    }
    std::cerr << "Invalid value for --" << flagname << ": " << value
              << std::endl;
    return false;
}
DEFINE_string(trans_protocol, trans_args[0],
              "transport protocol to use for"
              " passing messages");
DEFINE_validator(trans_protocol, &ValidateTransMode);

const std::string partitioner_args[] = {"default", "warehouse_dist_items",
                                        "warehouse"};
const partitioner_t parts[]{DEFAULT, WAREHOUSE_DIST_ITEMS, WAREHOUSE};
static bool ValidatePartitioner(const char *flagname,
                                const std::string &value)
{
    int n = sizeof(partitioner_args);
    for (int i = 0; i < n; ++i)
    {
        if (value == partitioner_args[i])
        {
            return true;
        }
    }
    std::cerr << "Invalid value for --" << flagname << ": " << value
              << std::endl;
    return false;
}
DEFINE_string(partitioner, partitioner_args[0],
              "the partitioner to use during this"
              " experiment");
DEFINE_validator(partitioner, &ValidatePartitioner);

/**
 * TPCC settings.
 */
DEFINE_int32(tpcc_num_warehouses, 1, "number of warehouses (for tpcc)");

/**
 * StrongStore settings.
 */
DEFINE_int64(strong_max_dep_depth, -1,
             "maximum length of dependency chain"
             " [-1 is no maximum] (for StrongStore MVTSO)");

const std::string strong_consistency_args[] = {"ss", "rss"};
const strongstore::Consistency strong_consistency[]{
    strongstore::Consistency::SS,
    strongstore::Consistency::RSS,
};
static bool ValidateStrongConsistency(const char *flagname,
                                      const std::string &value)
{
    int n = sizeof(strong_consistency_args);
    for (int i = 0; i < n; ++i)
    {
        if (value == strong_consistency_args[i])
        {
            return true;
        }
    }
    std::cerr << "Invalid value for --" << flagname << ": " << value
              << std::endl;
    return false;
}
DEFINE_string(strong_consistency, strong_consistency_args[0],
              "the consistency model to use during this"
              " experiment");
DEFINE_validator(strong_consistency, &ValidateStrongConsistency);

/**
 * Experiment settings.
 */
DEFINE_uint64(clock_error, 0, "maximum error for clock");
DEFINE_string(stats_file, "", "path to file for server stats");

/**
 * Benchmark settings.
 */
DEFINE_string(keys_path, "", "path to file containing keys in the system");
DEFINE_uint64(num_keys, 0, "number of keys to generate");
DEFINE_string(data_file_path, "",
              "path to file containing key-value pairs to be loaded");
DEFINE_bool(preload_keys, false, "load keys into server if generating keys");

Server *server = nullptr;
TransportReceiver *replica = nullptr;
::Transport *tport = nullptr;
Partitioner *part = nullptr;

void Cleanup(int signal);

int main(int argc, char **argv)
{
    gflags::SetUsageMessage(
        "runs a replica for a distributed replicated transaction\n"
        "           processing system.");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    Notice("Starting server.");

    TrueTime tt{FLAGS_clock_error};

    // parse replication configuration
    std::ifstream replica_config_stream(FLAGS_replica_config_path);
    if (replica_config_stream.fail())
    {
        std::cerr << "Unable to read configuration file: "
                  << FLAGS_replica_config_path << std::endl;
    }

    // parse shard configuration
    std::ifstream shard_config_stream(FLAGS_shard_config_path);
    if (shard_config_stream.fail())
    {
        std::cerr << "Unable to read configuration file: "
                  << FLAGS_shard_config_path << std::endl;
    }

    // parse protocol and mode
    protocol_t proto = PROTO_UNKNOWN;
    int numProtos = sizeof(protocol_args);
    for (int i = 0; i < numProtos; ++i)
    {
        if (FLAGS_protocol == protocol_args[i])
        {
            proto = protos[i];
            break;
        }
    }

    // parse consistency
    strongstore::Consistency consistency = strongstore::Consistency::SS;
    int n_consistencies = sizeof(strong_consistency);
    for (int i = 0; i < n_consistencies; ++i)
    {
        if (FLAGS_strong_consistency == strong_consistency_args[i])
        {
            consistency = strong_consistency[i];
            break;
        }
    }

    // parse transport protocol
    transmode_t trans = TRANS_UNKNOWN;
    int numTransModes = sizeof(trans_args);
    for (int i = 0; i < numTransModes; ++i)
    {
        if (FLAGS_trans_protocol == trans_args[i])
        {
            trans = transmodes[i];
            break;
        }
    }
    if (trans == TRANS_UNKNOWN)
    {
        std::cerr << "Unknown transport protocol." << std::endl;
        return 1;
    }

    transport::Configuration replica_config(replica_config_stream);
    if (FLAGS_replica_idx >= static_cast<uint64_t>(replica_config.n))
    {
        std::cerr << "Replica index " << FLAGS_replica_idx
                  << " is out of bounds"
                     "; only "
                  << replica_config.n << " replicas defined" << std::endl;
    }

    transport::Configuration shard_config(shard_config_stream);
    if (FLAGS_replica_idx >= static_cast<uint64_t>(shard_config.n))
    {
        std::cerr << "Replica index " << FLAGS_replica_idx
                  << " is out of bounds"
                     "; only "
                  << shard_config.n << " replicas defined" << std::endl;
    }

    if (proto == PROTO_UNKNOWN)
    {
        std::cerr << "Unknown protocol." << std::endl;
        return 1;
    }

    switch (trans)
    {
    case TRANS_TCP:
        tport = new TCPTransport(0.0, 0.0, 0, false);
        break;
    case TRANS_UDP:
        tport = new UDPTransport(0.0, 0.0, 0, false);
        break;
    default:
        NOT_REACHABLE();
    }

    // parse protocol and mode
    partitioner_t partType = DEFAULT;
    int numParts = sizeof(partitioner_args);
    for (int i = 0; i < numParts; ++i)
    {
        if (FLAGS_partitioner == partitioner_args[i])
        {
            partType = parts[i];
            break;
        }
    }

    std::mt19937 unused;
    switch (partType)
    {
    case DEFAULT:
        part = new DefaultPartitioner();
        break;
    case WAREHOUSE_DIST_ITEMS:
        part = new WarehouseDistItemsPartitioner(FLAGS_tpcc_num_warehouses);
        break;
    case WAREHOUSE:
        part = new WarehousePartitioner(FLAGS_tpcc_num_warehouses, unused);
        break;
    default:
        NOT_REACHABLE();
    }

    switch (proto)
    {
    case PROTO_STRONG:
    {
        server = new strongstore::Server(consistency, shard_config,
                                         replica_config, FLAGS_server_id,
                                         FLAGS_group_idx, FLAGS_replica_idx,
                                         tport, tt, FLAGS_debug_stats);
        break;
    }
    default:
    {
        NOT_REACHABLE();
    }
    }
    Debug("Created server");

    // parse keys
    size_t loaded = 0;
    size_t stored = 0;
    std::vector<int> txnGroups;
    if (FLAGS_data_file_path.empty() && FLAGS_keys_path.empty())
    {
        if (FLAGS_num_keys > 0)
        {
            if (FLAGS_preload_keys)
            {
                std::string key = "0000000000";
                for (size_t i = 0; i < FLAGS_num_keys; ++i)
                {
                    if ((*part)(key, FLAGS_num_shards, FLAGS_group_idx,
                                txnGroups) == FLAGS_group_idx)
                    {
                        server->Load(key, key, Timestamp());
                        ++stored;
                    }
                    if (i % 100000 == 0)
                    {
                        Debug("Loaded key %s", key.c_str());
                    }

                    ++loaded;

                    for (int j = key.size() - 1; j >= 0; --j)
                    {
                        if (key[j] < '9')
                        {
                            key[j] += static_cast<char>(1);
                            break;
                        }
                        else
                        {
                            key[j] = '0';
                        }
                    }
                }
            }

            Debug("Stored %lu out of %lu key-value pairs from [0,%lu).", stored,
                  loaded, FLAGS_num_keys);
        }
        else
        {
            std::cerr << "Specified neither keys file nor number of keys."
                      << std::endl;
            return 1;
        }
    }
    else if (FLAGS_data_file_path.length() > 0 && FLAGS_keys_path.empty())
    {
        std::ifstream in;
        in.open(FLAGS_data_file_path);
        if (!in)
        {
            std::cerr << "Could not read data from: " << FLAGS_data_file_path
                      << std::endl;
            return 1;
        }

        Debug("Populating with data from %s.", FLAGS_data_file_path.c_str());
        while (!in.eof())
        {
            std::string key;
            std::string value;
            int i = ReadBytesFromStream(&in, key);
            if (i == 0)
            {
                ReadBytesFromStream(&in, value);
                if ((*part)(key, FLAGS_num_shards, FLAGS_group_idx,
                            txnGroups) == FLAGS_group_idx)
                {
                    server->Load(key, value, Timestamp());
                    ++stored;
                }
                ++loaded;
            }
        }
        Debug("Stored %lu out of %lu key-value pairs from file %s.", stored,
              loaded, FLAGS_data_file_path.c_str());
    }
    else
    {
        std::ifstream in;
        in.open(FLAGS_keys_path);
        if (!in)
        {
            std::cerr << "Could not read keys from: " << FLAGS_keys_path
                      << std::endl;
            return 1;
        }
        std::string key;
        std::vector<int> txnGroups;
        while (std::getline(in, key))
        {
            if ((*part)(key, FLAGS_num_shards, FLAGS_group_idx, txnGroups) ==
                FLAGS_group_idx)
            {
                server->Load(key, "", Timestamp(0, 0));
            }
        }
        in.close();
    }
    Notice("Done loading server.");

    switch (proto)
    {
    case PROTO_STRONG:
    {
        replica = new replication::vr::VRReplica(
            replica_config, FLAGS_group_idx, FLAGS_replica_idx, tport, 1,
            dynamic_cast<replication::AppReplica *>(server),
            FLAGS_debug_stats);
        break;
    }
    default:
    {
        NOT_REACHABLE();
    }
    }
    Debug("Created replica");

    std::signal(SIGKILL, Cleanup);
    std::signal(SIGTERM, Cleanup);
    std::signal(SIGINT, Cleanup);

    CALLGRIND_START_INSTRUMENTATION;
    tport->Run();
    CALLGRIND_STOP_INSTRUMENTATION;
    CALLGRIND_DUMP_STATS;

    if (FLAGS_stats_file.size() > 0)
    {
        Notice("Exporting stats to %s.", FLAGS_stats_file.c_str());
        server->GetStats().ExportJSON(FLAGS_stats_file);
    }

    return 0;
}

void Cleanup(int signal)
{
    Notice("Gracefully exiting after signal %d.", signal);
    tport->Stop();
    if (FLAGS_stats_file.size() > 0)
    {
        Notice("Exporting stats to %s.", FLAGS_stats_file.c_str());
        server->GetStats().ExportJSON(FLAGS_stats_file);
    }
    delete replica;
    delete server;
    exit(0);
}
