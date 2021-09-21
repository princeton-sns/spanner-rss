import os
import json
import random
import numpy
import csv
import traceback
import subprocess
import concurrent
import collections
import operator
import math


def convert_latency_nanos_to_millis(latencies):
    return list(map(lambda x: x / 1e6, latencies))


def get_region(config, server):
    for region, servers in config["server_regions"].items():
        if server in servers:
            return region

    raise ValueError("{} not in any region".format(server))


def get_regions(config):
    return set([get_region(config, s) for s in config["clients"] + config["server_names"]])


def get_num_regions(config):
    return len(set([get_region(config, s) for s in config["clients"] + config["server_names"]]))


def calculate_statistics(config, local_out_directory):
    runs = []
    op_latencies = {}
    op_times = {}
    op_latency_counts = {}
    op_tputs = {}
    client_op_latencies = []
    client_op_times = []
    for i in range(config['num_experiment_runs']):
        client_op_latencies.append({})
        client_op_times.append({})
        stats, run_op_latencies, run_op_times, run_op_latency_counts, run_op_tputs, run_client_op_latencies, run_client_op_times = calculate_statistics_for_run(
            config, local_out_directory, i)
        runs.append(stats)

        for k, vv in run_op_latency_counts.items():
            for j in range(len(run_op_latencies[k])):
                if k in op_latencies:
                    if len(op_latencies[k]) <= j:
                        op_latencies[k].append(run_op_latencies[k][j])
                    else:
                        op_latencies[k][j] += run_op_latencies[k][j]
                else:
                    op_latencies[k] = [run_op_latencies[k][j]]
            if k in op_latency_counts:
                op_latency_counts[k] += vv
            else:
                op_latency_counts[k] = vv

        for k, v in run_op_times.items():
            for j in range(len(v)):
                if k in op_times:
                    if len(op_times[k]) <= j:
                        op_times[k].append(v[j])
                    else:
                        op_times[k][j] += v[j]
                else:
                    op_times[k] = [v[j]]

        for k, v in run_op_tputs.items():
            if k in run_op_tputs:
                if k in op_tputs:
                    op_tputs[k] += run_op_tputs[k]
                else:
                    op_tputs[k] = run_op_tputs[k]

        for cid, opl in run_client_op_latencies.items():
            client_op_latencies[i][cid] = opl
        for cid, opt in run_client_op_times.items():
            client_op_times[i][cid] = opt

    stats = {}
    stats['aggregate'] = {}
    norm_op_latencies, norm_op_times = calculate_all_op_statistics(
        config, stats['aggregate'], op_latencies, op_times, op_latency_counts, op_tputs)
    for k, v in norm_op_latencies.items():
        op_latencies['%s_norm' % k] = v
    for k, v in norm_op_times.items():
        op_times['%s_norm' % k] = v
    stats['runs'] = runs
    stats['run_stats'] = {}
    ignored = {'cdf': 1, 'cdf_log': 1, 'time': 1}
    for cat in runs[0]:  # we assume there is at least one run
        if not ('region-' in cat) and type(runs[0][cat]) is dict:
            stats['run_stats'][cat] = {}
            for s in runs[0][cat]:
                if s in ignored:
                    continue
                data = []
                for run in runs:
                    data.append(run[cat][s])
                stats['run_stats'][cat][s] = calculate_statistics_for_data(
                    data, cdf=False)
        if 'region-' in cat:
            stats['run_stats'][cat] = {}
            for cat2 in runs[0][cat]:  # we assume there is at least one run
                if type(runs[0][cat][cat2]) is dict:
                    stats['run_stats'][cat][cat2] = {}
                    for s in runs[0][cat][cat2]:
                        if s in ignored:
                            continue
                        data = []
                        for run in runs:
                            data.append(run[cat][cat2][s])
                        stats['run_stats'][cat][cat2][s] = calculate_statistics_for_data(
                            data, cdf=False)

    stats_file = STATS_FILE if 'stats_file_name' not in config else config['stats_file_name']
    with open(os.path.join(local_out_directory, stats_file), 'w') as f:
        json.dump(stats, f, indent=2, sort_keys=True)
    return stats, op_latencies, op_times, client_op_latencies, client_op_times


def calculate_statistics_for_run(config, local_out_directory, run):
    region_op_latencies = {}
    region_op_times = {}
    region_op_latency_counts = {}
    region_op_tputs = {}

    region_client_op_latencies = {}
    region_client_op_times = {}

    stats = {}

    regions = get_regions(config)
    for region in regions:
        r_op_latencies = {}
        r_op_latency_counts = {}
        r_op_times = {}
        r_op_tputs = {}
        op_latencies = {}
        op_latency_counts = {}
        op_tputs = {}
        op_times = {}
        for client in config["clients"]:
            if get_region(config, client) != region:
                continue

            client_dir = client
            for k in range(config["client_processes_per_client_node"]):
                client_out_file = os.path.join(local_out_directory,
                                               client_dir,
                                               '%s-%d-stdout-%d.log' % (client, k, run))
                start_time_sec = -1
                start_time_usec = -1
                end_time_sec = {}
                end_time_usec = {}
                with open(client_out_file) as f:
                    ops = f.readlines()
                    foundEnd = False
                    for op in ops:
                        foundEnd = False
                        opCols = op.strip().split(',')
                        for x in range(0, len(opCols), 2):
                            if opCols[x].isdigit():
                                break
                            if len(opCols[x]) > 0 and opCols[x][0] == '#':
                                # special line, not an operation
                                if opCols[x] == '#start':
                                    start_time_sec = float(opCols[x+1])
                                    start_time_usec = float(
                                        opCols[x+2])
                                    break
                                elif opCols[x] == '#end':
                                    cid = 0
                                    if x + 3 < len(opCols):
                                        cid = int(opCols[x+3])
                                    end_time_sec[cid] = float(
                                        opCols[x+1])
                                    end_time_usec[cid] = float(
                                        opCols[x+2])
                                    foundEnd = True
                                    break
                            if not opCols[x] in config['client_stats_blacklist']:
                                if 'input_latency_scale' in config:
                                    opLat = float(
                                        opCols[x+1]) / config['input_latency_scale']
                                else:
                                    opLat = float(opCols[x+1]) / 1e9
                                if 'output_latency_scale' in config:
                                    opLat = opLat * \
                                        config['output_latency_scale']
                                else:
                                    opLat = opLat * 1e3

                                opTime = 0.0
                                if x + 2 < len(opCols):
                                    if 'input_latency_scale' in config:
                                        opTime = float(
                                            opCols[x+2]) / config['input_latency_scale']
                                    else:
                                        opTime = float(
                                            opCols[x+2]) / 1e9
                                    if 'output_latency_scale' in config:
                                        opTime = opTime * \
                                            config['output_latency_scale']
                                    else:
                                        opTime = opTime * 1e3

                                cid = 0
                                if x + 3 < len(opCols):
                                    cid = int(opCols[x + 3])

                                if cid not in op_latencies:
                                    op_latencies[cid] = {}

                                if cid not in op_times:
                                    op_times[cid] = {}

                                if cid not in op_latency_counts:
                                    op_latency_counts[cid] = {}

                                if opCols[x] in op_latencies[cid]:
                                    op_latencies[cid][opCols[x]].append(
                                        opLat)
                                else:
                                    op_latencies[cid][opCols[x]] = [
                                        opLat]

                                if opCols[x] in op_times[cid]:
                                    op_times[cid][opCols[x]].append(
                                        opTime)
                                else:
                                    op_times[cid][opCols[x]] = [opTime]

                                if not opCols[x] in config['client_combine_stats_blacklist']:
                                    if 'combined' in op_latencies[cid]:
                                        op_latencies[cid]['combined'].append(
                                            opLat)
                                    else:
                                        op_latencies[cid]['combined'] = [
                                            opLat]

                                    if 'combined' in op_times[cid]:
                                        op_times[cid]['combined'].append(
                                            opTime)
                                    else:
                                        op_times[cid]['combined'] = [
                                            opTime]

                                    if 'combined' in op_latency_counts[cid]:
                                        op_latency_counts[cid]['combined'] += 1
                                    else:
                                        op_latency_counts[cid]['combined'] = 1

                                if 'client_combine_ro_ops' in config:
                                    if opCols[x] in config['client_combine_ro_ops']:
                                        rorw = 'ro'
                                    else:
                                        rorw = 'rw'

                                    if rorw in op_latencies[cid]:
                                        op_latencies[cid][rorw].append(
                                            opLat)
                                    else:
                                        op_latencies[cid][rorw] = [
                                            opLat]

                                    if rorw in op_times[cid]:
                                        op_times[cid][rorw].append(
                                            opTime)
                                    else:
                                        op_times[cid][rorw] = [
                                            opTime]

                                    if rorw in op_latency_counts[cid]:
                                        op_latency_counts[cid][rorw] += 1
                                    else:
                                        op_latency_counts[cid][rorw] = 1

                                if opCols[x] in op_latency_counts[cid]:
                                    op_latency_counts[cid][opCols[x]] += 1
                                else:
                                    op_latency_counts[cid][opCols[x]] = 1

                        if foundEnd:
                            run_time_sec = end_time_sec[cid]
                            run_time_sec += end_time_usec[cid] / 1e6
                            if cid in op_latency_counts:
                                for k1, v in op_latency_counts[cid].items():
                                    print('Client %s-%d %d tput %s is %f (%d / %f)' % (
                                        client, k, cid, k1, v / run_time_sec, v, run_time_sec))
                                    if k1 in op_tputs:
                                        op_tputs[k1] += v / \
                                            run_time_sec
                                    else:
                                        op_tputs[k1] = v / run_time_sec

                client_stats_file = os.path.join(local_out_directory,
                                                 client_dir,
                                                 '%s-%d-stats-%d.json' % (client, k, run))
                try:
                    with open(client_stats_file) as f:
                        client_stats = json.load(f)
                        for k1, v in client_stats.items():
                            if (not 'stats_merge_lists' in config) or (not k1 in config['stats_merge_lists']):
                                if k1 not in stats:
                                    stats[k1] = v
                                else:
                                    stats[k1] += v
                            else:
                                if k1 not in stats:
                                    stats[k1] = v
                                else:
                                    if len(stats[k1]) < len(v):
                                        for uu in range(len(stats[k1]), len(v)):
                                            stats[k1].append(0)
                                    for uu in range(len(v)):
                                        stats[k1][uu] += v[uu]

                except FileNotFoundError:
                    print('No stats file %s.' % client_stats_file)
                except json.decoder.JSONDecodeError:
                    print('Invalid JSON file %s.' % client_stats_file)

        for instance_idx in range(config["num_instances"]):
            for shard_idx in range(len(config["shards"])):
                shard = config["shards"][shard_idx]
                for replica_idx in range(len(shard)):
                    replica = shard[replica_idx]
                    if get_region(config, replica) != region:
                        continue

                    server_stats_file = os.path.join(local_out_directory, 'server-%d-%d' % (instance_idx, shard_idx),
                                                     'server-%d-%d-%d-stats-%d.json' % (instance_idx, shard_idx, replica_idx, run))
                    print(server_stats_file)
                    try:
                        with open(server_stats_file) as f:
                            server_stats = json.load(f)
                            for k, v in server_stats.items():
                                if not type(v) is dict:
                                    if (not 'stats_merge_lists' in config) or (not k in config['stats_merge_lists']):
                                        if k not in stats:
                                            stats[k] = v
                                        else:
                                            stats[k] += v
                                    else:
                                        if k not in stats:
                                            stats[k] = v
                                        else:
                                            if len(stats[k]) < len(v):
                                                for uu in range(len(stats[k]), len(v)):
                                                    stats[k].append(0)
                                            for uu in range(len(v)):
                                                stats[k][uu] += v[uu]
                    except FileNotFoundError:
                        print('No stats file %s.' % server_stats_file)
                    except json.decoder.JSONDecodeError:
                        print('Invalid JSON file %s.' % server_stats_file)

        for cid, opl in op_latencies.items():
            for k, v in opl.items():
                if k in r_op_latencies:
                    r_op_latencies[k].extend(v)
                else:
                    r_op_latencies[k] = v.copy()

            region_client_op_latencies[cid] = opl

        for cid, opt in op_times.items():
            for k, v in opt.items():
                if k in r_op_times:
                    r_op_times[k].extend(v)
                else:
                    r_op_times[k] = v.copy()

            region_client_op_times[cid] = opt

        for cid, oplc in op_latency_counts.items():
            for k, v in oplc.items():
                if k in r_op_latency_counts:
                    r_op_latency_counts[k] += v
                else:
                    r_op_latency_counts[k] = v

        for k, v in op_tputs.items():
            if k in r_op_tputs:
                r_op_tputs[k] += v
            else:
                r_op_tputs[k] = v

        # print('Region %d had op counts: w=%d, r=%d, rmw=%d.' % (i, writes, reads, rmws))
        # normalize by server region to account for latency differences
        for k, v in r_op_latencies.items():
            if k in region_op_latencies:
                region_op_latencies[k].append(v)
            else:
                region_op_latencies[k] = [v]
        for k, v in r_op_times.items():
            if k in region_op_times:
                region_op_times[k].append(v)
            else:
                region_op_times[k] = [v]
        for k, v in r_op_latency_counts.items():
            if k in region_op_latency_counts:
                region_op_latency_counts[k] = min(
                    region_op_latency_counts[k], v)
            else:
                region_op_latency_counts[k] = v
        for k, v in r_op_tputs.items():
            if k in region_op_tputs:
                region_op_tputs[k].append(v)
            else:
                region_op_tputs[k] = [v]

    # TODO: remove this hack
    if 'fast_writes_0' in stats or 'slow_writes_0' in stats or 'fast_reads_0' in stats or 'slow_reads_0' in stats:
        fw0 = stats['fast_writes_0'] if 'fast_writes_0' in stats else 0
        sw0 = stats['slow_writes_0'] if 'slow_writes_0' in stats else 0
        if fw0 + sw0 > 0:
            stats['fast_write_ratio'] = fw0 / (fw0 + sw0)
            stats['slow_write_ratio'] = sw0 / (fw0 + sw0)
        fr0 = stats['fast_reads_0'] if 'fast_reads_0' in stats else 0
        sr0 = stats['slow_reads_0'] if 'slow_reads_0' in stats else 0
        if fr0 + sr0 > 0:
            stats['fast_read_ratio'] = fr0 / (fr0 + sr0)
            stats['slow_read_ratio'] = sr0 / (fr0 + sr0)

    # TODO: decide if this is a hack that needs to be removed?
    total_committed = 0
    total_attempts = 0
    stats_new = {}
    for k, v in stats.items():
        if k.endswith('_committed'):
            k_prefix = k[:-len('_committed')]
            k_attempts = k_prefix + '_attempts'
            k_commit_rate = k_prefix + '_commit_rate'
            k_abort_rate = k_prefix + '_abort_rate'
            total_committed += stats[k]
            total_attempts += stats[k_attempts]
            stats_new[k_commit_rate] = stats[k] / stats[k_attempts]
            stats_new[k_abort_rate] = 1 - stats_new[k_commit_rate]

    for k, v in stats_new.items():
        stats[k] = v

    if total_attempts > 0:
        stats['committed'] = total_committed
        stats['attempts'] = total_attempts
        stats['commit_rate'] = total_committed / total_attempts
        stats['abort_rate'] = 1 - stats['commit_rate']

    norm_op_latencies, norm_op_times = calculate_all_op_statistics(
        config, stats, region_op_latencies, region_op_times, region_op_latency_counts, region_op_tputs)
    for k, v in norm_op_latencies.items():
        region_op_latencies['%s_norm' % k] = v
    for k, v in norm_op_times.items():
        region_op_times['%s_norm' % k] = v
    return stats, region_op_latencies, region_op_times, region_op_latency_counts, region_op_tputs, region_client_op_latencies, region_client_op_times


def calculate_op_statistics(config, stats, total_recorded_time, op_type, latencies, norm_latencies, tput):
    if len(latencies) > 0:
        stats[op_type] = calculate_statistics_for_data(latencies)
        stats[op_type]['ops'] = len(latencies)
        if tput == -1:
            stats[op_type]['tput'] = len(latencies) / total_recorded_time
        else:
            stats[op_type]['old_tput'] = len(latencies) / total_recorded_time
            stats[op_type]['tput'] = tput
        if op_type == 'combined':
            stats['combined']['ops'] = len(latencies)
            stats['combined']['time'] = total_recorded_time
        # TODO: fix
        # if (not 'server_emulate_wan' in config or config['server_emulate_wan']) and len(norm_latencies) > 0:
        #    stats['%s_norm' % op_type] = calculate_statistics_for_data(
        #        norm_latencies)
        #    stats['%s_norm' % op_type]['samples'] = len(norm_latencies)


def calculate_all_op_statistics(config, stats, region_op_latencies, region_op_times, region_op_latency_counts, region_op_tputs):
    total_recorded_time = float(
        config['client_experiment_length'] - config['client_ramp_up'] - config['client_ramp_down'])

    norm_op_latencies = {}
    norm_op_times = {}
    for k, v in region_op_latencies.items():
        latencies = [lat for region_lats in v for lat in region_lats]
        tput = -1 if len(v) == 0 else 0
        if k in region_op_tputs:
            for region_tput in region_op_tputs[k]:
                tput += region_tput

        for i in range(len(v)):
            sample_idxs = random.sample(
                range(len(v[i])), region_op_latency_counts[k])
            if k in norm_op_times:
                norm_op_latencies[k].extend([v[i][idx] for idx in sample_idxs])
                norm_op_times[k].extend(
                    [region_op_times[k][i][idx] for idx in sample_idxs])
            else:
                norm_op_latencies[k] = [v[i][idx] for idx in sample_idxs]
                norm_op_times[k] = [region_op_times[k][i][idx]
                                    for idx in sample_idxs]

        if not 'server_emulate_wan' in config or config['server_emulate_wan']:
            for i in range(len(v)):
                region_key = 'region-%d' % i
                if region_key not in stats:
                    stats[region_key] = {}
                op_tput = -1
                if k in region_op_tputs and len(region_op_tputs[k]) > i:
                    op_tput = region_op_tputs[k][i]
                calculate_op_statistics(
                    config, stats[region_key], total_recorded_time, k, v[i], [], op_tput)
        calculate_op_statistics(
            config, stats, total_recorded_time, k, latencies, norm_op_latencies[k], tput)
    return norm_op_latencies, norm_op_times


def calculate_cdf_for_npdata(npdata):
    ptiles = []
    for i in range(1, 100):  # compute percentiles [1, 100)
        ptiles.append([i, numpy.percentile(npdata, i, interpolation='higher')])
    return ptiles


def calculate_cdf_log_for_npdata(npdata, precision):
    ptiles = []
    base = 0
    scale = 1
    for i in range(0, precision):
        for j in range(0, 90):
            if i == 0 and j == 0:
                continue
            ptiles.append([base + j / scale, numpy.percentile(npdata,
                                                              base + j / scale, interpolation='higher')])
        base += 90 / scale
        scale = scale * 10
    return ptiles


def calculate_statistics_for_data(data, cdf=True, cdf_log_precision=4):
    npdata = numpy.asarray(data)
    s = {
        'p50': numpy.percentile(npdata, 50).item(),
        'p75': numpy.percentile(npdata, 75).item(),
        'p90': numpy.percentile(npdata, 90).item(),
        'p95': numpy.percentile(npdata, 95).item(),
        'p99': numpy.percentile(npdata, 99).item(),
        'p99.9': numpy.percentile(npdata, 99.9).item(),
        'max': numpy.amax(npdata).item(),
        'min': numpy.amin(npdata).item(),
        'mean': numpy.mean(npdata).item(),
        'stddev': numpy.std(npdata).item(),
        'var': numpy.var(npdata).item(),
    }
    if cdf:
        s['cdf'] = calculate_cdf_for_npdata(npdata)
        s['cdf_log'] = calculate_cdf_log_for_npdata(npdata, cdf_log_precision)
    return s


def generate_gnuplot_script_cdf_log_agg_new(script_file, out_file, x_label,
                                            y_label, width, height, font, series, title):
    with open(script_file, 'w') as f:
        write_gpi_header(f)
        f.write("set title \"%s\"\n" % title)
        f.write("set key bottom right\n")
        f.write("set ytics (0,0.9,0.99,0.999,0.9999,1.0)\n")
        f.write("set xlabel '%s'\n" % x_label)
        f.write("set ylabel '%s'\n" % y_label)
        f.write("set terminal pngcairo size %d,%d enhanced dashed font '%s'\n" % (
            width, height, font))
        f.write('set output \'%s\'\n' % out_file)
        write_line_styles(f)
        f.write('plot ')
        for i in range(len(series)):
            if i == 0:
                labels = ':yticlabels(3)'
            else:
                labels = ''
            f.write("'%s' using 1:(-log10(1-$2))%s title \"%s\" ls %d with lines" % (
                series[i][1], labels, series[i][0].replace('_', '\\\\\\_'), i + 1))
            if i != len(series) - 1:
                f.write(', \\\n')


def generate_gnuplot_script_lot_plot_stacked(script_file, out_file, x_label, y_label, width, height, font, series, title):
    with open(script_file, 'w') as f:
        write_gpi_header(f)
        f.write("set title \"%s\"\n" % title)
        f.write("set key top left\n")
        f.write("set xlabel '%s'\n" % x_label)
        f.write("set ylabel '%s'\n" % y_label)
        f.write("set terminal pngcairo size %d,%d enhanced dashed font '%s'\n" %
                (width, height, font))
        f.write('set output \'%s\'\n' % out_file)
        write_line_styles(f)
        f.write('plot ')
        for i in range(len(series)):
            f.write("'%s' title \"%s\" ls %d with filledcurves x1" % (series[i][1],
                                                                      series[i][0].replace('_', '\\\\\\_'), i + 1))
            if i != len(series) - 1:
                f.write(', \\\n')


def generate_gnuplot_script_cdf_agg_new(script_file, out_file, x_label, y_label, width, height, font, series, title):
    with open(script_file, 'w') as f:
        write_gpi_header(f)
        f.write("set title \"%s\"\n" % title)
        f.write("set key bottom right\n")
        f.write("set xlabel '%s'\n" % x_label)
        f.write("set ylabel '%s'\n" % y_label)
        f.write("set terminal pngcairo size %d,%d enhanced dashed font '%s'\n" %
                (width, height, font))
        f.write('set output \'%s\'\n' % out_file)
        write_line_styles(f)
        f.write('plot ')
        for i in range(len(series)):
            f.write("'%s' title \"%s\" ls %d with lines" % (series[i][1],
                                                            series[i][0].replace('_', '\\\\\\_'), i + 1))
            if i != len(series) - 1:
                f.write(', \\\n')


def generate_csv_for_plot(plot_csv_file, x_vars, y_vars):
    with open(plot_csv_file, 'w') as f:
        csvwriter = csv.writer(f)
        for i in range(len(x_vars)):
            csvwriter.writerow([x_vars[i], y_vars[i]])


def generate_gnuplot_script(plot, plot_script_file, plot_csv_file, plot_out_file):
    with open(plot_script_file, 'w') as f:
        write_gpi_header(f)
        f.write("set key top left\n")
        f.write("set xlabel '%s'\n" % plot['x_label'])
        f.write("set ylabel '%s'\n" % plot['y_label'])
        f.write("set terminal pngcairo size %d,%d enhanced font '%s'\n" %
                (plot['width'], plot['height'], plot['font']))
        f.write('set output \'%s\'\n' % plot_out_file)
        write_line_styles(f)
        f.write("plot '%s' title '%s' with linespoint\n" %
                (plot_csv_file, 'series-1'))


def generate_plot(plot, plots_directory, x_vars, y_vars):
    plot_csv_file = os.path.join(plots_directory, '%s.csv' % plot['name'])
    generate_csv_for_plot(plot_csv_file, x_vars, y_vars)
    plot_script_file = os.path.join(plots_directory, '%s.gpi' % plot['name'])
    plot_out_file = os.path.join(plots_directory, '%s.png' % plot['name'])
    generate_gnuplot_script(plot, plot_script_file,
                            plot_csv_file, plot_out_file)
    subprocess.call(['gnuplot', plot_script_file])


def generate_gnuplot_script_agg(plot, plot_script_file, plot_out_file, series):
    with open(plot_script_file, 'w') as f:
        write_gpi_header(f)
        f.write("set key top left\n")
        f.write("set xlabel '%s'\n" % plot['x_label'])
        f.write("set ylabel '%s'\n" % plot['y_label'])
        f.write("set terminal pngcairo size %d,%d enhanced dashed font '%s'\n" %
                (plot['width'], plot['height'], plot['font']))
        f.write('set output \'%s\'\n' % plot_out_file)
        write_line_styles(f)
        f.write('plot ')
        for i in range(len(series)):
            f.write("'%s' title '%s' ls %d with linespoint" % (
                series[i], plot['series_titles'][i].replace('_', '\\_'), i + 1))
            if i != len(series) - 1:
                f.write(', \\\n')


def generate_plots(config, base_out_directory, out_dirs):
    plots_directory = os.path.join(
        base_out_directory, config['plot_directory_name'])
    os.makedirs(plots_directory, exist_ok=True)
    csv_classes = set()
    csv_files = []
    subprocesses = []

    ###
    # Generate aggregate cdf plots
    for i in range(len(out_dirs[0])):
        # for each series i
        csv_files.append({})
        collecting = collections.deque([out_dirs[0][i]])
        while len(collecting) != 0:
            # bfs flattening of independent vars
            if type(collecting[0]) is str:
                sub_plot_directory = os.path.join(
                    collecting[0], config['plot_directory_name'])
                for f in os.listdir(sub_plot_directory):
                    if f.endswith('.csv') and (f.startswith('aggregate-') or f.startswith('lot-')):
                        csv_class = os.path.splitext(os.path.basename(f))[0]
                        csv_classes.add(csv_class)
                        if not csv_class in csv_files[i]:
                            csv_files[i][csv_class] = []
                        csv_files[i][csv_class].append(
                            os.path.join(sub_plot_directory, f))
            else:
                for od in collecting[0]:
                    collecting.append(od)
            collecting.popleft()

    for csv_class in csv_classes:
        idx = -1
        for j in range(len(csv_files)):
            if csv_class in csv_files[j]:
                idx = j
                break
        if idx == -1:
            continue
        for j in range(len(csv_files[idx][csv_class])):
            title = ''
            for k in range(len(config['experiment_independent_vars']) - 1 - len(config['experiment_independent_vars_unused']), -1, -1):
                title += '%s=%s' % (config['experiment_independent_vars'][k][0].replace('_', '\\\\\\_'),
                                    str(config[config['experiment_independent_vars'][k][0]]))
                if k > 0:
                    title += '\\n'
            x = j
            for k in range(len(config['experiment_independent_vars_unused']) - 1, 0, -1):
                if k == len(config['experiment_independent_vars_unused']) - 1:
                    title += '\\n'
                n = len(
                    config[config['experiment_independent_vars_unused'][k][0]])
                title += '%s=%s' % (config['experiment_independent_vars_unused'][k][0].replace('_', '\\\\\\_'),
                                    str(config[config['experiment_independent_vars_unused'][k][0]][x % n]))
                x = x // n
                if k > 0:
                    title += '\\n'
            plot_script_file = os.path.join(
                plots_directory, '%s-%d.gpi' % (csv_class, j))
            plot_out_file = os.path.join(
                plots_directory, '%s-%d.png' % (csv_class, j))
            series = []
            for i in range(len(csv_files)):
                if csv_class in csv_files[i] and len(csv_files[i][csv_class]) > j:
                    series.append(('%s=%s' % (config['experiment_independent_vars_unused'][0][0],
                                              config[config['experiment_independent_vars_unused'][0][0]][i]), csv_files[i][csv_class][j]))
            if 'lot-' in csv_class:
                if not 'lot_plots' in config:
                    config['lot_plots'] = {
                        'x_label': 'Time',
                        'y_label': 'Latency',
                        'width': config['cdf_plots']['width'],
                        'height': config['cdf_plots']['height'],
                        'font': config['cdf_plots']['font']
                    }
                generate_gnuplot_script_cdf_agg_new(plot_script_file,
                                                    plot_out_file, config['lot_plots']['x_label'],
                                                    config['lot_plots']['y_label'],
                                                    config['lot_plots']['width'],
                                                    config['lot_plots']['height'],
                                                    config['lot_plots']['font'], series, title)
            elif 'log' in csv_class:
                generate_gnuplot_script_cdf_log_agg_new(plot_script_file,
                                                        plot_out_file, config['cdf_plots']['x_label'],
                                                        config['cdf_plots']['y_label'],
                                                        config['cdf_plots']['width'],
                                                        config['cdf_plots']['height'],
                                                        config['cdf_plots']['font'], series, title)
            else:
                generate_gnuplot_script_cdf_agg_new(plot_script_file,
                                                    plot_out_file, config['cdf_plots']['x_label'],
                                                    config['cdf_plots']['y_label'],
                                                    config['cdf_plots']['width'],
                                                    config['cdf_plots']['height'],
                                                    config['cdf_plots']['font'], series, title)
            print(plot_script_file)
            # subprocesses.append(subprocess.Popen(['gnuplot', plot_script_file]))
            subprocess.call(['gnuplot', plot_script_file])
    # End generate all aggregate cdf plots
    ###

    ###
    # Generate specific plots
    #   for now we only support configurable plot generation with 1 indep var
    for plot in config['plots']:
        if len(config['experiment_independent_vars']) - len(config['experiment_independent_vars_unused']) == 1:
            # generate csvs and single series plots
            x_vars = []
            y_vars = []
            for i in range(len(out_dirs[0])):
                assert type(out_dirs[0][i]) is str
                # for each value of the independent variable
                stats_file = os.path.join(
                    out_dirs[0][i], config['stats_file_name'])
                print(stats_file)
                with open(stats_file) as f:
                    stats = json.load(f)
                    if plot['x_var_is_config']:
                        x_var = config
                        for k in plot['x_var']:
                            if type(x_var) is dict:
                                x_var = x_var[k]
                            elif type(x_var) is list:
                                x_var = x_var[i]
                        if type(x_var) is list:
                            x_var = x_var[i]
                    else:
                        x_var = stats
                        for k in plot['x_var']:
                            if k in x_var:
                                x_var = x_var[k]
                            else:
                                x_var = 0
                                break
                    x_vars.append(x_var)

                    y_var = stats
                    for k in plot['y_var']:
                        if k in y_var or (isinstance(y_var, list) and isinstance(k, int) and k < len(y_var)):
                            y_var = y_var[k]
                        else:
                            y_var = 0
                            break
                    y_vars.append(y_var)
            print(plots_directory)
            generate_plot(plot, plots_directory, x_vars, y_vars)
        elif len(config['experiment_independent_vars']) == len(config['experiment_independent_vars_unused']):
            csv_files = []
            for i in range(len(out_dirs[-1])):
                # for series i
                sub_plot_directory = os.path.join(
                    out_dirs[-1][i], config['plot_directory_name'])
                csv_files.append(os.path.join(
                    sub_plot_directory, '%s.csv' % plot['name']))

            plot_script_file = os.path.join(
                plots_directory, '%s.gpi' % plot['name'])
            plot_out_file = os.path.join(
                plots_directory, '%s.png' % plot['name'])
            generate_gnuplot_script_agg(
                plot, plot_script_file, plot_out_file, csv_files)
            subprocess.call(['gnuplot', plot_script_file])
            # subprocesses.append(subprocess.Popen(['gnuplot', plot_script_file]))

    # End generate specific plots
    ###
    # for subprocess in subprocesses:
    #    subprocess.wait()


def run_gnuplot(data_files, out_file, script_file):
    # print(script_file)
    args = ['gnuplot', '-e', "outfile='%s'" % out_file]
    for i in range(len(data_files)):
        args += ['-e', "datafile%d='%s'" % (i, data_files[i])]
    args.append(script_file)
    subprocess.call(args)


def generate_csv_for_cdf_plot(csv_file, cdf_data, log=False):
    with open(csv_file, 'w') as f:
        csvwriter = csv.writer(f)
        k = 1
        for i in range(len(cdf_data)):
            data = [cdf_data[i][1], cdf_data[i][0] / 100]
            if log and abs(cdf_data[i][0] / 100 - (1 - 10**-k)) < 0.000001:
                data.append(1 - 10**-k)
                k += 1
            csvwriter.writerow(data)


def generate_csv_for_lot_plot(csv_file, lot_data, lot_times=None, use_idxs=False):
    with open(csv_file, 'w') as f:
        csvwriter = csv.writer(f)
        if lot_times == None:
            agg = 0.0
            for i in range(len(lot_data)):
                agg += lot_data[i]
                if use_idxs:
                    data = [i, lot_data[i]]
                else:
                    data = [agg, lot_data[i]]
                csvwriter.writerow(data)
        else:
            aggregate_data = []
            for i in range(len(lot_data)):
                aggregate_data.append([lot_times[i], lot_data[i]])
            aggregate_data = sorted(aggregate_data, key=operator.itemgetter(0))
            for row in aggregate_data:
                csvwriter.writerow(row)


def generate_csv_for_tot_plot(csv_file, lot_data, lot_times):
    with open(csv_file, 'w') as f:
        csvwriter = csv.writer(f)
        aggregate_data = []
        for i in range(len(lot_data)):
            aggregate_data.append([lot_times[i], lot_data[i]])
        aggregate_data = sorted(aggregate_data, key=operator.itemgetter(0))

        tot_data = []
        if len(aggregate_data) > 0:
            ops_in_interval = 1
            start_interval = aggregate_data[0][0]
            end_interval = aggregate_data[0][0]
            interval = 5e2
            for i in range(1, len(aggregate_data)):
                if aggregate_data[i][0] < start_interval + interval:
                    ops_in_interval += 1
                else:
                    tot_data.append(
                        [end_interval, ops_in_interval * 1e3 / interval])
                    ops_in_interval = 0
                    start_interval = aggregate_data[i][0]
                end_interval = aggregate_data[i][0]
            tot_data.append([end_interval, ops_in_interval * 1e3 / interval])
        for row in tot_data:
            csvwriter.writerow(row)


def generate_cdf_plot(config, plots_directory, plot_name, cdf_data):
    plot_name = plot_name.replace('_', '-')
    plot_csv_file = os.path.join(plots_directory, '%s.csv' % plot_name)
    generate_csv_for_cdf_plot(plot_csv_file, cdf_data)
    plot_script_file = os.path.join(plots_directory, '%s.gpi' % plot_name)
    generate_gnuplot_script_cdf(config, plot_script_file)
    run_gnuplot([plot_csv_file], os.path.join(plots_directory, '%s.png' % plot_name),
                plot_script_file)


def generate_gnuplot_script_lot(config, script_file, line_type='points'):
    with open(script_file, 'w') as f:
        f.write("set datafile separator ','\n")
        f.write("set key bottom right\n")
        f.write("set yrange [0:]\n")
        if 'plot_lot_x_label' in config:
            f.write("set xlabel '%s'\n" % config['plot_lot_x_label'])
        if 'plot_lot_y_label' in config:
            f.write("set ylabel '%s'\n" % config['plot_lot_y_label'])
        f.write("set terminal pngcairo size %d,%d enhanced font '%s'\n" %
                (config['plot_cdf_png_width'], config['plot_cdf_png_height'],
                 config['plot_cdf_png_font']))
        f.write('set output outfile\n')
        f.write("plot datafile0 title '%s' with %s\n" % (config['plot_cdf_series_title'].replace('_', '\\_'),
                                                         line_type))


def generate_lot_plot(config, plots_directory, plot_name, lot_data, lot_times):
    plot_name = plot_name.replace('_', '-')
    plot_csv_file = os.path.join(plots_directory, '%s.csv' % plot_name)
    generate_csv_for_lot_plot(plot_csv_file, lot_data, lot_times)
    plot_script_file = os.path.join(plots_directory, '%s.gpi' % plot_name)
    generate_gnuplot_script_lot(config, plot_script_file)
    run_gnuplot([plot_csv_file], os.path.join(plots_directory, '%s.png' % plot_name),
                plot_script_file)


def generate_tot_plot(config, plots_directory, plot_name, lot_data, lot_times):
    plot_name = plot_name.replace('_', '-')
    plot_csv_file = os.path.join(plots_directory, '%s.csv' % plot_name)
    generate_csv_for_tot_plot(plot_csv_file, lot_data, lot_times)
    plot_script_file = os.path.join(plots_directory, '%s.gpi' % plot_name)
    generate_gnuplot_script_lot(config, plot_script_file, 'linespoints')
    run_gnuplot([plot_csv_file], os.path.join(plots_directory, '%s.png' % plot_name),
                plot_script_file)


def generate_cdf_log_plot(config, plots_directory, plot_name, cdf_data):
    plot_name = plot_name.replace('_', '-')
    plot_csv_file = os.path.join(plots_directory, '%s.csv' % plot_name)
    generate_csv_for_cdf_plot(plot_csv_file, cdf_data, log=True)
    plot_script_file = os.path.join(plots_directory, '%s.gpi' % plot_name)
    generate_gnuplot_script_cdf_log(config, plot_script_file)
    run_gnuplot([plot_csv_file], os.path.join(plots_directory, '%s.png' % plot_name),
                plot_script_file)


CDF_PLOTS = ['txn', 'w', 'r', 'rmw', 'max', 'maxr', 'maxw', 'maxrmw', 'combined', 'txn_norm',
             'w_norm', 'r_norm', 'rmw_norm', 'combined_norm', 'max_norm', 'maxr_norm', 'maxw_norm', 'maxrmw_norm']


def generate_cdf_plots(config, local_out_directory, stats, executor):
    futures = []
    plots_directory = os.path.join(
        local_out_directory, config['plot_directory_name'])
    os.makedirs(plots_directory, exist_ok=True)
    for op_type in stats['aggregate']:
        if not op_type in config['client_cdf_plot_blacklist'] and not 'region-' in op_type:
            cdf_plot_name = 'aggregate-%s' % op_type
            futures.append(executor.submit(generate_cdf_plot, config, plots_directory, cdf_plot_name,
                                           stats['aggregate'][op_type]['cdf']))
            cdf_log_plot_name = 'aggregate-%s-log' % op_type
            futures.append(executor.submit(generate_cdf_log_plot, config, plots_directory, cdf_log_plot_name,
                                           stats['aggregate'][op_type]['cdf_log']))
        elif 'region-' in op_type:
            for op_type2 in stats['aggregate'][op_type]:
                if not op_type2 in config['client_cdf_plot_blacklist']:
                    cdf_plot_name = 'aggregate-%s-%s' % (op_type, op_type2)
                    futures.append(executor.submit(generate_cdf_plot, config, plots_directory, cdf_plot_name,
                                                   stats['aggregate'][op_type][op_type2]['cdf']))
                    cdf_log_plot_name = 'aggregate-%s-%s-log' % (
                        op_type, op_type2)
                    futures.append(executor.submit(generate_cdf_log_plot, config, plots_directory, cdf_log_plot_name,
                                                   stats['aggregate'][op_type][op_type2]['cdf_log']))

    for i in range(len(stats['runs'])):
        for op_type in stats['runs'][i]:
            if not op_type in config['client_cdf_plot_blacklist'] and not 'region-' in op_type:
                if type(stats['runs'][i][op_type]) is dict:
                    cdf_plot_name = 'run-%d-%s' % (i, op_type)
                    futures.append(executor.submit(generate_cdf_plot, config, plots_directory, cdf_plot_name,
                                                   stats['runs'][i][op_type]['cdf']))
                    cdf_log_plot_name = 'run-%d-%s-log' % (i, op_type)
                    futures.append(executor.submit(generate_cdf_log_plot, config, plots_directory, cdf_log_plot_name,
                                                   stats['runs'][i][op_type]['cdf']))
            elif 'region-' in op_type:
                for op_type2 in stats['runs'][i][op_type]:
                    if not op_type2 in config['client_cdf_plot_blacklist']:
                        if type(stats['runs'][i][op_type]) is dict:
                            cdf_plot_name = 'run-%d-%s-%s' % (
                                i, op_type, op_type2)
                            futures.append(executor.submit(generate_cdf_plot, config, plots_directory, cdf_plot_name,
                                                           stats['runs'][i][op_type][op_type2]['cdf']))
                            cdf_log_plot_name = 'run-%d-%s-%s-log' % (
                                i, op_type, op_type2)
                            futures.append(executor.submit(generate_cdf_log_plot, config, plots_directory, cdf_log_plot_name,
                                                           stats['runs'][i][op_type][op_type2]['cdf']))
    concurrent.futures.wait(futures)


def generate_ot_plots(config, local_out_directory, stats, op_latencies, op_times, client_op_latencies, client_op_times, executor):
    futures = []
    plots_directory = os.path.join(
        local_out_directory, config['plot_directory_name'])
    os.makedirs(plots_directory, exist_ok=True)

    for i in range(len(stats['runs'])):
        ops = []
        for op_type in stats['runs'][i]:
            if config['client_total'] == 1 and op_type.startswith('op'):
                ops.append(op_type)
            if not op_type in config['client_cdf_plot_blacklist'] and not 'region-' in op_type:
                if type(stats['runs'][i][op_type]) is dict:
                    plot_name = 'run-%d-%s' % (i, op_type)
                    futures.append(executor.submit(generate_lot_plot, config,
                                                   plots_directory, 'lot-' + plot_name,
                                                   op_latencies[op_type][i], op_times[op_type][i]))
                    futures.append(executor.submit(generate_tot_plot, config,
                                                   plots_directory, 'tot-' + plot_name,
                                                   op_latencies[op_type][i], op_times[op_type][i]))
                    for cid, opl in client_op_latencies[i].items():
                        if not op_type in client_op_times[i][cid] or not op_type in opl:
                            continue
                        client_plot_name = plot_name + ('-client-%d' % cid)
                        futures.append(executor.submit(generate_lot_plot, config,
                                                       plots_directory, 'lot-' + client_plot_name,
                                                       opl[op_type], client_op_times[i][cid][op_type]))
                        futures.append(executor.submit(generate_tot_plot, config,
                                                       plots_directory, 'tot-' + client_plot_name,
                                                       opl[op_type], client_op_times[i][cid][op_type]))
            elif 'region-' in op_type:
                for op_type2 in stats['runs'][i][op_type]:
                    if not op_type2 in config['client_cdf_plot_blacklist']:
                        if type(stats['runs'][i][op_type]) is dict:
                            lot_plot_name = 'lot-run-%d-%s-%s' % (
                                i, op_type, op_type2)
                            futures.append(executor.submit(generate_lot_plot,
                                                           config, plots_directory, lot_plot_name,
                                                           op_latencies[op_type2][i], op_times[op_type2][i]))
        if len(ops) > 0:
            ops.sort()
            series = [convert_latency_nanos_to_millis(
                stats['runs'][i][ops[0]])]
            series_csvs = []
            plot_csv_file = os.path.join(plots_directory, '%s.csv' % ops[0])
            series_csvs.append((ops[0], plot_csv_file))
            generate_csv_for_lot_plot(
                plot_csv_file, series[0], None, None, True)
            for k in range(1, len(ops)):
                series.append(convert_latency_nanos_to_millis(
                    stats['runs'][i][ops[k]]))
                for j in range(len(series[-2])):
                    series[-1][j] += series[-2][j]
                plot_csv_file = os.path.join(
                    plots_directory, '%s.csv' % ops[k])
                series_csvs.append((ops[k], plot_csv_file))
                generate_csv_for_lot_plot(
                    plot_csv_file, series[-1], None, None, True)

            series.append(convert_latency_nanos_to_millis(
                stats['runs'][i]['commit']))
            for j in range(len(series[-2])):
                series[-1][j] += series[-2][j]

            plot_csv_file = os.path.join(plots_directory, 'commit.csv')
            series_csvs.append(('commit', plot_csv_file))
            generate_csv_for_lot_plot(
                plot_csv_file, series[-1], None, None, True)

            plot_name = 'breakdown'
            plot_script_file = os.path.join(
                plots_directory, '%s.gpi' % plot_name)
            plot_out_file = os.path.join(plots_directory, '%s.png' % plot_name)
            series_csvs.reverse()
            generate_gnuplot_script_lot_plot_stacked(plot_script_file, plot_out_file,
                                                     'Transaction #', config['lot_plots']['y_label'],
                                                     1600, 600,
                                                     config['lot_plots']['font'], series_csvs, 'Breakdown')
            subprocess.call(['gnuplot', plot_script_file])

    for fut in concurrent.futures.as_completed(futures):
        fut.result()


def generate_csv_for_tput_lat_plot(plot_csv_file, tputs, lats):
    with open(plot_csv_file, 'w') as f:
        csvwriter = csv.writer(f)
        for i in range(len(tputs)):
            csvwriter.writerow([tputs[i], lats[i]])


def generate_tput_lat_plot(config, plots_directory, plot_name, tputs, lats):
    plot_csv_file = os.path.join(plots_directory, '%s.csv' % plot_name)
    generate_csv_for_tput_lat_plot(plot_csv_file, tputs, lats)
    plot_script_file = os.path.join(plots_directory, '%s.gpi' % plot_name)
    generate_gnuplot_script_tput_lat(config, plot_script_file)
    run_gnuplot([plot_csv_file], os.path.join(plots_directory,
                                              '%s.png' % plot_name), plot_script_file)


STATS_FILE = 'stats.json'


def generate_tput_lat_plots(config, base_out_directory, exp_out_directories):
    plots_directory = os.path.join(
        base_out_directory, config['plot_directory_name'])
    os.makedirs(plots_directory, exist_ok=True)
    tputs = []
    lats = {}
    for i in range(len(exp_out_directories)):
        stats_file = os.path.join(exp_out_directories[i], STATS_FILE)
        print(stats_file)
        with open(stats_file) as f:
            stats = json.load(f)
            if 'combined' in stats['run_stats']:
                combined_run_stats = stats['run_stats']['combined']
                tputs.append(combined_run_stats['tput']['p50'])
                ignore = {'stddev': 1, 'var': 1, 'tput': 1, 'ops': 1}
                for lat_stat, lat in combined_run_stats.items():
                    if lat_stat in ignore:
                        continue
                    if lat_stat not in lats:
                        lats[lat_stat] = []
                    lats[lat_stat].append(lat['p50'])  # median of runs
    for lat_stat, lat in lats.items():
        plot_name = 'tput-%s-lat' % lat_stat
        print(plots_directory)
        generate_tput_lat_plot(config, plots_directory, plot_name, tputs, lat)


def generate_agg_cdf_plots(config, base_out_directory, sub_out_directories):
    plots_directory = os.path.join(
        base_out_directory, config['plot_directory_name'])
    os.makedirs(plots_directory, exist_ok=True)
    csv_files = {}
    for i in range(len(sub_out_directories)):
        # for replication protocol i
        for j in range(len(sub_out_directories[i])):
            # for client configuration j
            sub_plot_directory = os.path.join(
                sub_out_directories[i][j], config['plot_directory_name'])
            for f in os.listdir(sub_plot_directory):
                if f.endswith('.csv') and f.startswith('aggregate'):
                    csv_class = os.path.splitext(os.path.basename(f))[0]
                    if csv_class not in csv_files:
                        csv_files[csv_class] = []
                    if len(csv_files[csv_class]) == j:
                        csv_files[csv_class].append([])
                    csv_files[csv_class][j].append(
                        os.path.join(sub_plot_directory, f))

    for csv_class, file_lists in csv_files.items():
        for j in range(len(file_lists)):
            plot_script_file = os.path.join(
                plots_directory, '%s-%d.gpi' % (csv_class, j))
            if 'log' in csv_class:
                generate_gnuplot_script_cdf_log_agg(config, plot_script_file)
            else:
                generate_gnuplot_script_cdf_agg(config, plot_script_file)
            run_gnuplot(file_lists[j], os.path.join(plots_directory,
                                                    '%s-%d.png' % (csv_class, j)), plot_script_file)


def generate_agg_tput_lat_plots(config, base_out_directory, out_directories):
    plots_directory = os.path.join(
        base_out_directory, config['plot_directory_name'])
    os.makedirs(plots_directory, exist_ok=True)
    csv_files = {}
    for i in range(len(out_directories)):
        # for replication protocol i
        sub_plot_directory = os.path.join(
            out_directories[i], config['plot_directory_name'])
        for f in os.listdir(sub_plot_directory):
            if f.endswith('.csv'):
                csv_class = os.path.splitext(os.path.basename(f))[0]
                if csv_class not in csv_files:
                    csv_files[csv_class] = []
                csv_files[csv_class].append(
                    os.path.join(sub_plot_directory, f))

    for csv_class, files in csv_files.items():
        plot_script_file = os.path.join(plots_directory, '%s.gpi' % csv_class)
        generate_gnuplot_script_tput_lat_agg(config, plot_script_file)
        run_gnuplot(files, os.path.join(plots_directory, '%s.png' %
                                        csv_class), plot_script_file)


def generate_gnuplot_script_tail_at_scale(config, script_file):
    with open(script_file, 'w') as f:
        write_gpi_header(f)
        f.write("set key top left\n")
        f.write("set xlabel '# of subrequests'\n")
        f.write("set ylabel 'Median Latency (ms)'\n")
        f.write("set terminal pngcairo size %d,%d enhanced dashed font '%s'\n" %
                (config['plot_cdf_png_width'], config['plot_cdf_png_height'],
                 config['plot_cdf_png_font']))
        f.write('set output outfile\n')
        write_line_styles(f)
        f.write('plot ')
        for i in range(len(config['replication_protocol'])):
            f.write("datafile%d title '%s' ls %d with lines" % (
                i, config['plot_cdf_series_title'][i].replace('_', '\\_'), i + 1))
            if i != len(config['replication_protocol']) - 1:
                f.write(', \\\n')


def generate_tail_at_scale_plots(config, base_out_directory, sub_out_directories):
    plots_directory = os.path.join(
        base_out_directory, config['plot_directory_name'])
    os.makedirs(plots_directory, exist_ok=True)
    csv_files = [[[] for i in range(len(config['server_names'])+1)]
                 for j in range(len(config['client_nodes_per_server']))]
    for k in range(len(config['client_tail_at_scale'])):
        # for tail-at-scale request size k
        for i in range(len(sub_out_directories[k])):
            # for replication protocol i
            for j in range(len(sub_out_directories[k][i])):
                # for client configuration j
                with open(os.path.join(sub_out_directories[k][i][j], STATS_FILE)) as f:
                    stats = json.load(f)
                    csvfile = os.path.join(
                        plots_directory, '%s-%d-%d-overall.csv' % (config['replication_protocol'][i], i, j))
                    csv_files[j][0].append(csvfile)
                    with open(csvfile, 'a') as csvf:
                        csvwriter = csv.writer(csvf)
                        csvwriter.writerow(
                            [config['client_tail_at_scale'][k], stats['aggregate']['max_norm']['p50']])
                        for r in range(len(config['server_names'])):
                            csvfilereg = os.path.join(
                                plots_directory, '%s-%d-%d-region-%d.csv' % (config['replication_protocol'][i], i, j, r))
                            csv_files[j][r+1].append(csvfilereg)
                            with open(csvfilereg, 'a') as csvfreg:
                                csvwriterreg = csv.writer(csvfreg)
                                csvwriterreg.writerow(
                                    [config['client_tail_at_scale'][k], stats['aggregate']['region-%d' % r]['max']['p50']])

    for j in range(len(config['client_nodes_per_server'])):
        plot_script_prefix = 'tail-at-scale-overall-%d' % j
        plot_script_file = os.path.join(
            plots_directory, '%s.gpi' % plot_script_prefix)
        generate_gnuplot_script_tail_at_scale(config, plot_script_file)
        run_gnuplot(csv_files[j][0], os.path.join(
            plots_directory, '%s.png' % plot_script_prefix), plot_script_file)
        print(csv_files)
        num_regions = get_num_regions(config)
        for r in range(num_regions):
            plot_script_file = os.path.join(
                plots_directory, 'tail-at-scale-%d-region-%d.gpi' % (j, r))
            generate_gnuplot_script_tail_at_scale(config, plot_script_file)
            run_gnuplot(csv_files[j][r+1], os.path.join(plots_directory,
                                                        'tail-at-scale-%d-region-%d.png' % (j, r)), plot_script_file)


def generate_gnuplot_script_cdf(config, script_file):
    with open(script_file, 'w') as f:
        f.write("set datafile separator ','\n")
        f.write("set key bottom right\n")
        f.write("set xlabel '%s'\n" % config['plot_cdf_x_label'])
        f.write("set ylabel '%s'\n" % config['plot_cdf_y_label'])
        f.write("set terminal pngcairo size %d,%d enhanced font '%s'\n" %
                (config['plot_cdf_png_width'], config['plot_cdf_png_height'],
                 config['plot_cdf_png_font']))
        f.write('set output outfile\n')
        f.write("plot datafile0 title '%s' with lines\n" %
                config['plot_cdf_series_title'].replace('_', '\\_'))


def generate_gnuplot_script_cdf_log(config, script_file):
    with open(script_file, 'w') as f:
        f.write("set datafile separator ','\n")
        f.write("set key bottom right\n")
        f.write("set xlabel '%s'\n" % config['plot_cdf_x_label'])
        f.write("set ylabel '%s'\n" % config['plot_cdf_y_label'])
        f.write("set terminal pngcairo size %d,%d enhanced font '%s'\n" %
                (config['plot_cdf_png_width'], config['plot_cdf_png_height'],
                 config['plot_cdf_png_font']))
        f.write('set output outfile\n')
        f.write("plot datafile0 using 1:(-log10(1-$2)):yticlabels(3) title '%s' with lines\n" %
                config['plot_cdf_series_title'].replace('_', '\\_'))


def generate_gnuplot_script_tput_lat(config, plot_script_file):
    with open(plot_script_file, 'w') as f:
        f.write("set datafile separator ','\n")
        f.write("set key top left\n")
        f.write("set xlabel '%s'\n" % config['plot_tput_lat_x_label'])
        f.write("set ylabel '%s'\n" % config['plot_tput_lat_y_label'])
        f.write("set terminal pngcairo size %d,%d enhanced font '%s'\n" %
                (config['plot_tput_lat_png_width'],
                 config['plot_tput_lat_png_height'],
                 config['plot_tput_lat_png_font']))
        f.write('set output outfile\n')
        f.write("plot datafile0 title '%s' with linespoint\n" %
                config['plot_tput_lat_series_title'].replace('_', '\\_'))


def write_gpi_header(f):
    f.write("set datafile separator ','\n")


def write_line_styles(f):
    f.write('set style line 1 linetype 1 linewidth 2\n')
    f.write('set style line 2 linetype 1 linecolor "green" linewidth 2\n')
    f.write('set style line 3 linetype 1 linecolor "blue" linewidth 2\n')
    f.write('set style line 4 linetype 4 linewidth 2\n')
    f.write('set style line 5 linetype 5 linewidth 2\n')
    f.write('set style line 6 linetype 8 linewidth 2\n')


def generate_gnuplot_script_cdf_log_agg(config, script_file):
    with open(script_file, 'w') as f:
        write_gpi_header(f)
        f.write("set key bottom right\n")
        f.write("set ytics (0,0.9,0.99,0.999,0.9999,1.0)\n")
        f.write("set xlabel '%s'\n" % config['plot_cdf_x_label'])
        f.write("set ylabel '%s'\n" % config['plot_cdf_y_label'])
        f.write("set terminal pngcairo size %d,%d enhanced dashed font '%s'\n" %
                (config['plot_cdf_png_width'], config['plot_cdf_png_height'],
                 config['plot_cdf_png_font']))
        f.write('set output outfile\n')
        write_line_styles(f)
        f.write('plot ')
        for i in range(len(config['replication_protocol'])):
            if i == 0:
                labels = ':yticlabels(3)'
            else:
                labels = ''
            f.write("datafile%d using 1:(-log10(1-$2))%s title '%s' ls %d with lines" %
                    (i, labels, config['plot_cdf_series_title'][i].replace('_', '\\_'), i + 1))
            if i != len(config['replication_protocol']) - 1:
                f.write(', \\\n')


def generate_gnuplot_script_cdf_agg(config, script_file):
    with open(script_file, 'w') as f:
        write_gpi_header(f)
        f.write("set key bottom right\n")
        f.write("set xlabel '%s'\n" % config['plot_cdf_x_label'])
        f.write("set ylabel '%s'\n" % config['plot_cdf_y_label'])
        f.write("set terminal pngcairo size %d,%d enhanced dashed font '%s'\n" %
                (config['plot_cdf_png_width'], config['plot_cdf_png_height'],
                 config['plot_cdf_png_font']))
        f.write('set output outfile\n')
        write_line_styles(f)
        f.write('plot ')
        for i in range(len(config['replication_protocol'])):
            f.write("datafile%d title '%s' ls %d with lines" % (
                i, config['plot_cdf_series_title'][i].replace('_', '\\_'), i + 1))
            if i != len(config['replication_protocol']) - 1:
                f.write(', \\\n')


def generate_gnuplot_script_tput_lat_agg(config, plot_script_file):
    with open(plot_script_file, 'w') as f:
        write_gpi_header(f)
        f.write("set key top left\n")
        f.write("set xlabel '%s'\n" % config['plot_tput_lat_x_label'])
        f.write("set ylabel '%s'\n" % config['plot_tput_lat_y_label'])
        f.write("set terminal pngcairo size %d,%d enhanced dashed font '%s'\n" %
                (config['plot_tput_lat_png_width'],
                 config['plot_tput_lat_png_height'],
                 config['plot_tput_lat_png_font']))
        f.write('set output outfile\n')
        write_line_styles(f)
        f.write('plot ')
        for i in range(len(config['replication_protocol'])):
            f.write("datafile%d title '%s' ls %d with linespoint" % (
                i, config['plot_tput_lat_series_title'][i].replace('_', '\\_'), i + 1))
            if i != len(config['replication_protocol']) - 1:
                f.write(', \\\n')


def regenerate_plots(config_file, exp_dir, executor, calc_stats=True):
    with open(config_file) as f:
        config = json.load(f)

        if not 'client_stats_blacklist' in config:
            config['client_stats_blacklist'] = []
        if not 'client_combine_stats_blacklist' in config:
            config['client_combine_stats_blacklist'] = []
        if not 'client_cdf_plot_blacklist' in config:
            config['client_cdf_plot_blacklist'] = []

        out_directories = sorted(next(os.walk(exp_dir))[1])
        if 'plots' in out_directories:
            out_directories.remove('plots')
        out_directories = [os.path.join(exp_dir, d) for d in out_directories]
        out_directories = out_directories[:len(config['replication_protocol'])]
        sub_out_directories = []
        for i in range(len(out_directories)):
            out_dir = out_directories[i]
            dirs = sorted(next(os.walk(out_dir))[1])
            if 'plots' in dirs:
                dirs.remove('plots')
            dirs = [os.path.join(
                out_dir, d, config['out_directory_name']) for d in dirs]
            config_new = config.copy()
            config_new['base_local_exp_directory'] = exp_dir
            server_replication_protocol = config['replication_protocol'][i]
            config_new['replication_protocol'] = server_replication_protocol
            config_new['plot_cdf_series_title'] = config['plot_cdf_series_title'][i]
            config_new['plot_tput_lat_series_title'] = config['plot_tput_lat_series_title'][i]
            config_new['replication_protocol_settings'] = config['replication_protocol_settings'][i]
            sub_out_directories.append(dirs)
            for j in range(len(dirs)):
                sub_out_dir = dirs[j]
                config_new_new = config_new.copy()
                config_new_new['base_local_exp_directory'] = exp_dir
                n = config_new['client_nodes_per_server'][j]
                m = config_new['client_processes_per_client_node'][j]
                config_new_new['client_nodes_per_server'] = n
                config_new_new['client_processes_per_client_node'] = m
                if calc_stats:
                    stats = calculate_statistics(config_new_new, sub_out_dir)
                else:
                    with open(os.path.join(sub_out_dir, STATS_FILE)) as f:
                        stats = json.load(f)
                generate_cdf_plots(
                    config_new_new, sub_out_dir, stats, executor)
            generate_tput_lat_plots(config_new, out_dir, dirs)
        generate_agg_cdf_plots(config, exp_dir, sub_out_directories)
        generate_agg_tput_lat_plots(config, exp_dir, out_directories)


def generate_agg_write_percentage_csv(config, base_out_directory, sub_out_directories):
    plots_directory = os.path.join(
        base_out_directory, config['plot_directory_name'])
    os.makedirs(plots_directory, exist_ok=True)
    print(config['client_write_percentage'])
    write_percentage = config['client_write_percentage'] / (
        config['client_write_percentage'] + config['client_read_percentage'] + config['client_rmw_percentage'])
    for i in range(len(sub_out_directories)):
        # for replication protocol i
        for j in range(len(sub_out_directories[i])):
            # for client configuration j
            with open(os.path.join(sub_out_directories[i][j], STATS_FILE)) as f:
                stats = json.load(f)
                for p in ['p50', 'p75', 'p90', 'p95', 'p99']:
                    for t in ['w_norm', 'r_norm']:
                        with open(os.path.join(base_out_directory, '%s-%d-%d-%s-%s.csv' % (config['replication_protocol'][i], i, j, t, p)), 'w') as f:
                            csvwriter = csv.writer(f)
                            csvwriter.writerow(
                                [write_percentage, stats['aggregate'][t][p]])


def generate_varying_write_csvs(config_file, exp_dir, calc_stats=True):
    with open(config_file) as f:
        config = json.load(f)
        out_directories = sorted(next(os.walk(exp_dir))[1])
        if 'plots' in out_directories:
            out_directories.remove('plots')
        out_directories = [os.path.join(exp_dir, d) for d in out_directories]
        out_directories = out_directories[:len(config['replication_protocol'])]
        sub_out_directories = []
        for i in range(len(out_directories)):
            out_dir = out_directories[i]
            dirs = sorted(next(os.walk(out_dir))[1])
            if 'plots' in dirs:
                dirs.remove('plots')
            dirs = [os.path.join(
                out_dir, d, config['out_directory_name']) for d in dirs]
            config_new = config.copy()
            config_new['base_local_exp_directory'] = exp_dir
            server_replication_protocol = config['replication_protocol'][i]
            config_new['replication_protocol'] = server_replication_protocol
            config_new['plot_cdf_series_title'] = config['plot_cdf_series_title'][i]
            config_new['plot_tput_lat_series_title'] = config['plot_tput_lat_series_title'][i]
            config_new['replication_protocol_settings'] = config['replication_protocol_settings'][i]
            sub_out_directories.append(dirs)
            for j in range(len(dirs)):
                sub_out_dir = dirs[j]
                config_new_new = config_new.copy()
                config_new_new['base_local_exp_directory'] = exp_dir
                n = config_new['client_nodes_per_server'][j]
                m = config_new['client_processes_per_client_node'][j]
                config_new_new['client_nodes_per_server'] = n
                config_new_new['client_processes_per_client_node'] = m
        generate_agg_write_percentage_csv(config, exp_dir, sub_out_directories)


def regenerate_tail_at_scale_plots(config_file, exp_dir):
    with open(config_file) as f:
        config = json.load(f)
        directories = sorted(next(os.walk(exp_dir))[1])
        if 'plots' in directories:
            directories.remove('plots')
        directories = [os.path.join(exp_dir, d) for d in directories]
        directories = directories[:len(config['client_tail_at_scale'])]
        sub_sub_out_directories = []
        for k in range(len(directories)):
            out_directories = sorted(next(os.walk(directories[k]))[1])
            if 'plots' in out_directories:
                out_directories.remove('plots')
            out_directories = [os.path.join(
                directories[k], d) for d in out_directories]
            out_directories = out_directories[:len(
                config['replication_protocol'])]
            sub_out_directories = []
            for i in range(len(out_directories)):
                out_dir = out_directories[i]
                dirs = sorted(next(os.walk(out_dir))[1])
                if 'plots' in dirs:
                    dirs.remove('plots')
                dirs = [os.path.join(
                    out_dir, d, config['out_directory_name']) for d in dirs]
                config_new = config.copy()
                config_new['client_tail_at_scale'] = config['client_tail_at_scale'][k]
                config_new['base_local_exp_directory'] = exp_dir
                server_replication_protocol = config['replication_protocol'][i]
                config_new['replication_protocol'] = server_replication_protocol
                config_new['plot_cdf_series_title'] = config['plot_cdf_series_title'][i]
                config_new['plot_tput_lat_series_title'] = config['plot_tput_lat_series_title'][i]
                config_new['replication_protocol_settings'] = config['replication_protocol_settings'][i]
                sub_out_directories.append(dirs)
            sub_sub_out_directories.append(sub_out_directories)
        generate_tail_at_scale_plots(config, exp_dir, sub_sub_out_directories)
