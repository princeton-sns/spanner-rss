// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/benchmark/tpccClient.cc:
 *   Benchmarking client for tpcc.
 *
 **********************************************************************/

#include <gflags/gflags.h>
#include <valgrind/callgrind.h>

#include <algorithm>
#include <atomic>
#include <csignal>
#include <sstream>
#include <thread>
#include <vector>

#include "lib/latency.h"
#include "lib/tcptransport.h"
#include "lib/timeval.h"
#include "store/benchmark/async/bench_client.h"
#include "store/benchmark/async/common/key_selector.h"
#include "store/benchmark/async/common/uniform_key_selector.h"
#include "store/benchmark/async/common/zipf_key_selector.h"
#include "store/benchmark/async/retwis/retwis_client.h"
#include "store/common/partitioner.h"
#include "store/common/stats.h"
#include "store/common/truetime.h"
#include "store/strongstore/client.h"
#include "store/strongstore/networkconfig.h"

enum protomode_t {
    PROTO_UNKNOWN,
    PROTO_STRONG,
};

enum benchmode_t {
    BENCH_UNKNOWN,
    BENCH_RETWIS,
};

enum keysmode_t { KEYS_UNKNOWN,
                  KEYS_UNIFORM,
                  KEYS_ZIPF };

enum transmode_t {
    TRANS_UNKNOWN,
    TRANS_UDP,
    TRANS_TCP,
};

/**
 * System settings.
 */
DEFINE_uint64(client_id, 0, "unique identifier for client");
DEFINE_string(client_host, "", "client host string");
DEFINE_string(replica_config_paths, "", "paths to replication configuration files");
DEFINE_uint64(num_shards, 1, "number of shards in the system");
DEFINE_bool(ping_replicas, false, "determine latency to replicas via pings");
DEFINE_string(net_config_path, "", "path to network configuration file");

DEFINE_bool(debug_stats, false, "record stats related to debugging");

const std::string trans_args[] = {"udp", "tcp"};

const transmode_t transmodes[]{TRANS_UDP, TRANS_TCP};
static bool ValidateTransMode(const char *flagname, const std::string &value) {
    int n = sizeof(trans_args);
    for (int i = 0; i < n; ++i) {
        if (value == trans_args[i]) {
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

const std::string protocol_args[] = {"span-lock"};
const protomode_t protomodes[]{PROTO_STRONG};
const strongstore::Mode strongmodes[]{strongstore::Mode::MODE_SPAN_LOCK};
static bool ValidateProtocolMode(const char *flagname,
                                 const std::string &value) {
    int n = sizeof(protocol_args);
    for (int i = 0; i < n; ++i) {
        if (value == protocol_args[i]) {
            return true;
        }
    }
    std::cerr << "Invalid value for --" << flagname << ": " << value
              << std::endl;
    return false;
}
DEFINE_string(protocol_mode, protocol_args[0],
              "the mode of the protocol to"
              " use during this experiment");
DEFINE_validator(protocol_mode, &ValidateProtocolMode);

const std::string strong_consistency_args[] = {"ss", "rss"};
const strongstore::Consistency strong_consistency[]{
    strongstore::Consistency::SS,
    strongstore::Consistency::RSS,
};
static bool ValidateStrongConsistency(const char *flagname,
                                      const std::string &value) {
    int n = sizeof(strong_consistency_args);
    for (int i = 0; i < n; ++i) {
        if (value == strong_consistency_args[i]) {
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

DEFINE_double(nb_time_alpha, 1.0, "multiple for non-block time estimates.");

const std::string benchmark_args[] = {"retwis"};
const benchmode_t benchmodes[]{BENCH_RETWIS};
static bool ValidateBenchmark(const char *flagname, const std::string &value) {
    int n = sizeof(benchmark_args);
    for (int i = 0; i < n; ++i) {
        if (value == benchmark_args[i]) {
            return true;
        }
    }
    std::cerr << "Invalid value for --" << flagname << ": " << value
              << std::endl;
    return false;
}
DEFINE_string(benchmark, benchmark_args[0],
              "the mode of the protocol to use"
              " during this experiment");
DEFINE_validator(benchmark, &ValidateBenchmark);

/**
 * Experiment settings.
 */
DEFINE_uint64(exp_duration, 30, "duration (in seconds) of experiment");
DEFINE_uint64(warmup_secs, 5,
              "time (in seconds) to warm up system before"
              " recording stats");
DEFINE_uint64(cooldown_secs, 5,
              "time (in seconds) to cool down system after"
              " recording stats");
DEFINE_uint64(tput_interval, 0,
              "time (in seconds) between throughput"
              " measurements");
DEFINE_uint64(num_requests, -1,
              "number of requests (transactions) per"
              " client");
DEFINE_int32(closest_replica, -1, "index of the replica closest to the client");
DEFINE_string(closest_replicas, "",
              "space-separated list of replica indices in"
              " order of proximity to client(s)");
DEFINE_uint64(delay, 0, "maximum time to wait between client operations");
DEFINE_uint64(clock_error, 0, "maximum error for clock");
DEFINE_string(stats_file, "", "path to output stats file.");
DEFINE_uint64(abort_backoff, 100,
              "sleep exponentially increasing amount after abort.");
DEFINE_bool(retry_aborted, true, "retry aborted transactions.");
DEFINE_int64(max_attempts, -1,
             "max number of attempts per transaction (or -1"
             " for unlimited).");
DEFINE_uint64(message_timeout, 10000, "length of timeout for messages in ms.");
DEFINE_uint64(max_backoff, 5000, "max time to sleep after aborting.");

const std::string partitioner_args[] = {"default", "warehouse_dist_items",
                                        "warehouse"};
const partitioner_t parts[]{DEFAULT, WAREHOUSE_DIST_ITEMS, WAREHOUSE};
static bool ValidatePartitioner(const char *flagname,
                                const std::string &value) {
    int n = sizeof(partitioner_args);
    for (int i = 0; i < n; ++i) {
        if (value == partitioner_args[i]) {
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
 * Retwis settings.
 */
DEFINE_string(keys_path, "",
              "path to file containing keys in the system"
              " (for retwis)");
DEFINE_uint64(num_keys, 0, "number of keys to generate (for retwis");

const std::string keys_args[] = {"uniform", "zipf"};
const keysmode_t keysmodes[]{KEYS_UNIFORM, KEYS_ZIPF};
static bool ValidateKeys(const char *flagname, const std::string &value) {
    int n = sizeof(keys_args);
    for (int i = 0; i < n; ++i) {
        if (value == keys_args[i]) {
            return true;
        }
    }
    std::cerr << "Invalid value for --" << flagname << ": " << value
              << std::endl;
    return false;
}
DEFINE_string(key_selector, keys_args[0],
              "the distribution from which to "
              "select keys.");
DEFINE_validator(key_selector, &ValidateKeys);

const std::string bench_args[] = {"open", "closed"};

const BenchmarkClientMode bench_modes[]{OPEN, CLOSED};
static bool ValidateBenchMode(const char *flagname, const std::string &value) {
    int n = sizeof(bench_args);
    for (int i = 0; i < n; ++i) {
        if (value == bench_args[i]) {
            return true;
        }
    }
    std::cerr << "Invalid value for --" << flagname << ": " << value
              << std::endl;
    return false;
}
DEFINE_string(bench_mode, bench_args[0], "benchmark mode (open or closed)");
DEFINE_validator(bench_mode, &ValidateBenchMode);

DEFINE_double(zipf_coefficient, 0.5, "the coefficient of the zipf distribution for key selection.");
DEFINE_double(client_arrival_rate, 1.0, "arrival rate for open loop clients");
DEFINE_double(client_think_time, 1.0, "think time for closed and partly open loop clients");
DEFINE_double(client_stay_probability, 0.5, "session stay probability for partly open loop clients");
DEFINE_double(mpl, 1, "multi-programming level for closed-loop clients");
DEFINE_double(client_switch_probability, 0.0, "session switch service probability for multi-instance experiments");

/**
 * RW settings.
 */
DEFINE_uint64(num_ops_txn, 1,
              "number of ops in each txn"
              " (for rw)");
// RW benchmark also uses same config parameters as Retwis.

/**
 * TPCC settings.
 */
DEFINE_int32(warehouse_per_shard, 1,
             "number of warehouses per shard"
             " (for tpcc)");
DEFINE_int32(clients_per_warehouse, 1,
             "number of clients per warehouse"
             " (for tpcc)");
DEFINE_int32(remote_item_milli_p, 0, "remote item milli p (for tpcc)");

DEFINE_int32(tpcc_num_warehouses, 1, "number of warehouses (for tpcc)");
DEFINE_int32(tpcc_w_id, 1, "home warehouse id for this client (for tpcc)");
DEFINE_int32(tpcc_C_c_id, 1,
             "C value for NURand() when selecting"
             " random customer id (for tpcc)");
DEFINE_int32(tpcc_C_c_last, 1,
             "C value for NURand() when selecting"
             " random customer last name (for tpcc)");
DEFINE_int32(tpcc_new_order_ratio, 45,
             "ratio of new_order transactions to other"
             " transaction types (for tpcc)");
DEFINE_int32(tpcc_delivery_ratio, 4,
             "ratio of delivery transactions to other"
             " transaction types (for tpcc)");
DEFINE_int32(tpcc_stock_level_ratio, 4,
             "ratio of stock_level transactions to other"
             " transaction types (for tpcc)");
DEFINE_int32(tpcc_payment_ratio, 43,
             "ratio of payment transactions to other"
             " transaction types (for tpcc)");
DEFINE_int32(tpcc_order_status_ratio, 4,
             "ratio of order_status transactions to other"
             " transaction types (for tpcc)");
DEFINE_bool(static_w_id, false,
            "force clients to use same w_id for each treansaction");

/**
 * Smallbank settings.
 */

DEFINE_int32(balance_ratio, 60,
             "percentage of balance transactions"
             " (for smallbank)");
DEFINE_int32(deposit_checking_ratio, 10,
             "percentage of deposit checking"
             " transactions (for smallbank)");
DEFINE_int32(transact_saving_ratio, 10,
             "percentage of transact saving"
             " transactions (for smallbank)");
DEFINE_int32(amalgamate_ratio, 10,
             "percentage of deposit checking"
             " transactions (for smallbank)");
DEFINE_int32(write_check_ratio, 10,
             "percentage of write check transactions"
             " (for smallbank)");
DEFINE_int32(num_hotspots, 1000, "# of hotspots (for smallbank)");
DEFINE_int32(num_customers, 18000, "# of customers (for smallbank)");
DEFINE_double(hotspot_probability, 0.9, "probability of ending in hotspot");
DEFINE_int32(timeout, 5000, "timeout in ms (for smallbank)");
DEFINE_string(customer_name_file_path, "smallbank_names",
              "path to file"
              " containing names to be loaded (for smallbank)");

DEFINE_LATENCY(op);

std::vector<Client *> clients;
std::vector<BenchmarkClient *> benchClients;
std::vector<std::thread *> threads;
Transport *tport;
Partitioner *part;
KeySelector *keySelector;

void Signal(int signal);
void Cleanup();
void FlushStats();

int main(int argc, char **argv) {
    gflags::SetUsageMessage(
        "executes transactions from various transactional workload\n"
        "           benchmarks against various distributed replicated "
        "transaction\n"
        "           processing systems.");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    Notice("Starting benchmark.");

    TrueTime tt{FLAGS_clock_error};

    // parse transport protocol
    transmode_t trans = TRANS_UNKNOWN;
    int numTransModes = sizeof(trans_args);
    for (int i = 0; i < numTransModes; ++i) {
        if (FLAGS_trans_protocol == trans_args[i]) {
            trans = transmodes[i];
            break;
        }
    }
    if (trans == TRANS_UNKNOWN) {
        std::cerr << "Unknown transport protocol." << std::endl;
        return 1;
    }

    // parse protocol and mode
    protomode_t mode = PROTO_UNKNOWN;
    strongstore::Mode strongmode = strongstore::Mode::MODE_UNKNOWN;
    int numProtoModes = sizeof(protocol_args);
    for (int i = 0; i < numProtoModes; ++i) {
        if (FLAGS_protocol_mode == protocol_args[i]) {
            mode = protomodes[i];
            strongmode = strongmodes[i];
            break;
        }
    }
    if (mode == PROTO_UNKNOWN ||
        (mode == PROTO_STRONG &&
         strongmode == strongstore::Mode::MODE_UNKNOWN)) {
        std::cerr << "Unknown protocol or unknown strongmode." << std::endl;
        return 1;
    }

    // parse consistency
    strongstore::Consistency consistency = strongstore::Consistency::SS;
    int n_consistencies = sizeof(strong_consistency);
    for (int i = 0; i < n_consistencies; ++i) {
        if (FLAGS_strong_consistency == strong_consistency_args[i]) {
            consistency = strong_consistency[i];
            break;
        }
    }

    // parse benchmark mode
    BenchmarkClientMode bench_mode = UNKNOWN;
    int numBenchModes = sizeof(bench_args);
    for (int i = 0; i < numBenchModes; ++i) {
        if (FLAGS_bench_mode == bench_args[i]) {
            bench_mode = bench_modes[i];
            break;
        }
    }
    if (bench_mode == UNKNOWN) {
        std::cerr << "Unknown bench mode." << std::endl;
        return 1;
    }

    // parse benchmark
    benchmode_t benchMode = BENCH_UNKNOWN;
    int numBenchs = sizeof(benchmark_args);
    for (int i = 0; i < numBenchs; ++i) {
        if (FLAGS_benchmark == benchmark_args[i]) {
            benchMode = benchmodes[i];
            break;
        }
    }
    if (benchMode == BENCH_UNKNOWN) {
        std::cerr << "Unknown benchmark." << std::endl;
        return 1;
    }

    // parse partitioner
    partitioner_t partType = DEFAULT;
    int numParts = sizeof(partitioner_args);
    for (int i = 0; i < numParts; ++i) {
        if (FLAGS_partitioner == partitioner_args[i]) {
            partType = parts[i];
            break;
        }
    }

    // parse key selector
    keysmode_t keySelectionMode = KEYS_UNKNOWN;
    int numKeySelectionModes = sizeof(keys_args);
    for (int i = 0; i < numKeySelectionModes; ++i) {
        if (FLAGS_key_selector == keys_args[i]) {
            keySelectionMode = keysmodes[i];
            break;
        }
    }
    if (keySelectionMode == KEYS_UNKNOWN) {
        std::cerr << "Unknown key selector." << std::endl;
        return 1;
    }

    // parse closest replicas
    std::vector<int> closestReplicas;
    std::stringstream iss(FLAGS_closest_replicas);
    int replica;
    iss >> replica;
    while (!iss.fail()) {
        closestReplicas.push_back(replica);
        iss >> replica;
    }

    // parse retwis settings
    std::vector<std::string> keys;
    if (benchMode == BENCH_RETWIS) {
        if (FLAGS_keys_path.empty()) {
            if (FLAGS_num_keys > 0) {
                std::string key = "0000000000";
                keys.reserve(FLAGS_num_keys);
                for (size_t i = 0; i < FLAGS_num_keys; ++i) {
                    keys.emplace_back(key);
                    for (int j = key.size() - 1; j >= 0; --j) {
                        if (key[j] < '9') {
                            key[j] += static_cast<char>(1);
                            break;
                        } else {
                            key[j] = '0';
                        }
                    }
                }
            } else {
                std::cerr << "Specified neither keys file nor number of keys."
                          << std::endl;
                return 1;
            }
        } else {
            std::ifstream in;
            in.open(FLAGS_keys_path);
            if (!in) {
                std::cerr << "Could not read keys from: " << FLAGS_keys_path
                          << std::endl;
                return 1;
            }
            std::string key;
            while (std::getline(in, key)) {
                keys.push_back(key);
            }
            in.close();
        }
    }

    switch (trans) {
        case TRANS_TCP:
            tport = new TCPTransport(0.0, 0.0, 0, false);
            break;
        case TRANS_UDP:
            tport = new UDPTransport(0.0, 0.0, 0, false);
            break;
        default:
            NOT_REACHABLE();
    }

    switch (keySelectionMode) {
        case KEYS_UNIFORM:
            keySelector = new UniformKeySelector(keys);
            break;
        case KEYS_ZIPF:
            keySelector = new ZipfKeySelector(keys, FLAGS_zipf_coefficient);
            break;
        default:
            NOT_REACHABLE();
    }

    std::mt19937 rand(FLAGS_client_id);  // TODO: is this safe?

    switch (partType) {
        case DEFAULT:
            part = new DefaultPartitioner();
            break;
        case WAREHOUSE_DIST_ITEMS:
            part = new WarehouseDistItemsPartitioner(FLAGS_tpcc_num_warehouses);
            break;
        case WAREHOUSE:
            part = new WarehousePartitioner(FLAGS_tpcc_num_warehouses, rand);
            break;
        default:
            NOT_REACHABLE();
    }

    std::string latencyFile;
    std::string latencyRawFile;
    std::vector<uint64_t> latencies;
    std::atomic<size_t> clientsDone(0UL);

    bench_done_callback bdcb = [&clientsDone, &latencyFile, &latencyRawFile]() {
        ++clientsDone;
        Debug("%lu clients have finished.", clientsDone.load());
        if (clientsDone == 1) {
            Latency_t sum;
            _Latency_Init(&sum, "total");
            for (unsigned int i = 0; i < benchClients.size(); i++) {
                Latency_Sum(&sum, &benchClients[i]->latency);
            }

            Latency_Dump(&sum);
            if (latencyFile.size() > 0) {
                Latency_FlushTo(latencyFile.c_str());
            }

            latencyRawFile = latencyFile + ".raw";
            std::ofstream rawFile(latencyRawFile.c_str(),
                                  std::ios::out | std::ios::binary);
            for (auto x : benchClients) {
                rawFile.write((char *)&x->latencies[0],
                              (x->latencies.size() * sizeof(x->latencies[0])));
                if (!rawFile) {
                    Warning("Failed to write raw latency output");
                }
            }
            tport->Stop();
        }
    };

    std::ifstream net_config_stream(FLAGS_net_config_path);
    if (net_config_stream.fail()) {
        std::cerr << "Unable to read configuration file: " << FLAGS_net_config_path << std::endl;
        return -1;
    }

    std::string buf;
    std::stringstream f{FLAGS_replica_config_paths};

    std::vector<transport::Configuration> replica_configs;
    std::vector<strongstore::NetworkConfiguration> net_configs;
    std::vector<std::string> client_regions;
    int i = 0;
    while (std::getline(f, buf, ',')) {
        std::ifstream replica_config_stream{buf};
        if (replica_config_stream.fail()) {
            std::cerr << "Unable to read configuration file: " << buf << std::endl;
            return -1;
        }
        replica_configs.emplace_back(replica_config_stream);

        if (mode == PROTO_STRONG) {
            net_config_stream.seekg(0);
            net_configs.emplace_back(replica_configs[i], net_config_stream);
            client_regions.emplace_back(net_configs[i].GetRegion(FLAGS_client_host));
        }

        i++;
    }

    // TODO: Remove this
    // if (closestReplicas.size() > 0 &&
    //     closestReplicas.size() != static_cast<size_t>(replica_config.n)) {
    //     std::cerr << "If specifying closest replicas, must specify all "
    //               << replica_config.n << "; only specified "
    //               << closestReplicas.size() << std::endl;
    //     return 1;
    // }

    const std::size_t n_instances = replica_configs.size();
    for (std::size_t i = 0; i < n_instances; ++i) {
        Client *client = nullptr;
        switch (mode) {
            case PROTO_STRONG: {
                auto &shard_config = replica_configs[i];
                auto &net_config = net_configs[i];
                auto &client_region = client_regions[i];

                client = new strongstore::Client(
                    consistency, net_config, client_region, shard_config,
                    FLAGS_client_id, FLAGS_num_shards, FLAGS_closest_replica,
                    tport, part, tt, FLAGS_debug_stats, FLAGS_nb_time_alpha);
                break;
            }
            default:
                NOT_REACHABLE();
        }

        ASSERT(client != nullptr);
        clients.push_back(client);
    }

    switch (benchMode) {
        case BENCH_RETWIS:
            break;
        default:
            NOT_REACHABLE();
    }

    uint32_t seed = FLAGS_client_id << 4;
    BenchmarkClient *bench;
    switch (benchMode) {
        case BENCH_RETWIS:
            bench = new retwis::RetwisClient(
                keySelector, clients, FLAGS_message_timeout, *tport, seed,
                bench_mode,
                FLAGS_client_switch_probability,
                FLAGS_client_arrival_rate, FLAGS_client_think_time, FLAGS_client_stay_probability,
                FLAGS_mpl,
                FLAGS_exp_duration, FLAGS_warmup_secs, FLAGS_cooldown_secs,
                FLAGS_tput_interval,
                FLAGS_abort_backoff, FLAGS_retry_aborted, FLAGS_max_backoff,
                FLAGS_max_attempts);
            break;
        default:
            NOT_REACHABLE();
    }

    switch (benchMode) {
        case BENCH_RETWIS:
            tport->Timer(0, [bench, bdcb]() { bench->Start(bdcb); });
            break;
        case BENCH_UNKNOWN:
        default:
            NOT_REACHABLE();
    }

    benchClients.push_back(bench);

    if (threads.size() > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }

    tport->Timer(FLAGS_exp_duration * 1000 - 1000, FlushStats);
    // tport->Timer(FLAGS_exp_duration * 1000, Cleanup);

    std::signal(SIGKILL, Signal);
    std::signal(SIGTERM, Signal);
    std::signal(SIGINT, Signal);

    CALLGRIND_START_INSTRUMENTATION;
    tport->Run();
    CALLGRIND_STOP_INSTRUMENTATION;
    CALLGRIND_DUMP_STATS;

    Notice("Cleaning up after experiment.");

    FlushStats();

    delete keySelector;
    for (auto i : threads) {
        i->join();
        delete i;
    }
    for (auto i : clients) {
        delete i;
    }
    for (auto i : benchClients) {
        delete i;
    }
    delete tport;
    delete part;

    return 0;
}

void Signal(int signal) {
    Notice("Gracefully stopping bench clients after signal %d.", signal);
    tport->Stop();
    Cleanup();
}

void Cleanup() {
    // tport->Stop();
}

void FlushStats() {
    if (FLAGS_stats_file.size() > 0) {
        Notice("Flushing stats to %s.", FLAGS_stats_file.c_str());
        Stats total;
        for (unsigned int i = 0; i < benchClients.size(); i++) {
            total.Merge(benchClients[i]->GetStats());
        }
        for (unsigned int i = 0; i < clients.size(); i++) {
            total.Merge(clients[i]->GetStats());
        }

        total.ExportJSON(FLAGS_stats_file);
    }
}
