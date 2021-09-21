import subprocess
import time
import os
import shutil


def get_master_host(config):
    return config['server_host_format_str'] % (config['master_server_name'],
                                               config['experiment_name'], config['project_name'])


def get_server_host(config, i):
    if isinstance(i, int):
        return config['server_host_format_str'] % (config['server_names'][i], config['experiment_name'], config['project_name'])
    elif isinstance(i, str):
        return config['server_host_format_str'] % (i, config['experiment_name'], config['project_name'])
    else:
        raise ValueError("Unexpected value for i: {}".format(i))


def get_client_host(config, client):
    return config['client_host_format_str'] % (client, config['experiment_name'],
                                               config['project_name'])


def get_ip_for_interface(interface, remote_user, remote_host):
    return run_remote_command_sync('ip address show %s | awk \'/inet / {print $2}\'' % interface, remote_user, remote_host).rstrip()


def run_local_command_sync(command):
    print(command)
    subprocess.run(command, stdout=subprocess.PIPE,
                   universal_newlines=True, shell=True)


def run_local_command_async(command):
    print(command)
    return subprocess.Popen(command, universal_newlines=True, shell=True)


def ssh_args(command, remote_user, remote_host):
    return ["ssh", '-o', 'StrictHostKeyChecking=no',
            '-o', 'ControlMaster=auto',
            '-o', 'ControlPersist=2m',
            '-o', 'ControlPath=~/.ssh/cm-%r@%h:%p',
            '%s@%s' % (remote_user, remote_host), command]


def run_remote_command_sync(command, remote_user, remote_host):
    print("{}@{}: {}".format(remote_user, remote_host, command))
    return subprocess.run(ssh_args(command, remote_user, remote_host),
                          stdout=subprocess.PIPE, universal_newlines=True).stdout


def run_remote_command_async(command, remote_user, remote_host, detach=True):
    print("{}@{}: {}".format(remote_user, remote_host, command))
    if detach:
        command = '(%s) >& /dev/null & exit' % command
    return subprocess.Popen(ssh_args(command, remote_user, remote_host))


def change_mounted_fs_permissions(remote_group, remote_user, remote_host, remote_path):
    run_remote_command_sync('sudo chown %s %s; sudo chmod 775 %s' % (
        remote_user, remote_path, remote_path), remote_user, remote_host)


def copy_path_to_remote_host(local_path, remote_user,
                             remote_host, remote_path, exclude_paths=[]):
    print('%s:%s' % (remote_host, remote_path))
    args = ["rsync", "-r", "-e", "ssh", local_path,
            '%s@%s:%s' % (remote_user, remote_host, remote_path)]
    if exclude_paths is not None:
        for i in range(len(exclude_paths)):
            args.append('--exclude')
            args.append(exclude_paths[i])
    subprocess.call(args)


def copy_remote_directory_to_local(local_directory, remote_user, remote_host, remote_directory, tar_file='logs.tar', file_filter='.'):
    os.makedirs(local_directory, exist_ok=True)
    tar_file_path = os.path.join(remote_directory, tar_file)
    run_remote_command_sync('cd %s && tar -czf %s %s' % (remote_directory, tar_file_path, file_filter),
                            remote_user, remote_host)
    subprocess.call(["scp", "-r", "-p", '%s@%s:%s' %
                     (remote_user, remote_host, tar_file_path), local_directory])
    subprocess.call(['tar', '-xzf', os.path.join(local_directory, tar_file),
                     '-C', local_directory])
    subprocess.call(['rm', '-rf', os.path.join(local_directory, tar_file)])


def tcsh_redirect_output_to_files(command, stdout_file, stderr_file):
    return '(%s > %s) >& %s' % (command, stdout_file, stderr_file)


def set_file_descriptor_limit(limit, remote_user, remote_host):
    command = "echo '%s soft nofile %d' | sudo tee -a /etc/security/limits.conf ; " % (
        remote_user, limit)
    command += "echo '%s hard nofile %d' | sudo tee -a /etc/security/limits.conf" % (
        remote_user, limit)
    run_remote_command_sync(command, remote_user, remote_host)


def kill_remote_process_by_name_cmd(remote_process_name, kill_args):
    cmd = 'pkill%s %s' % (kill_args, remote_process_name)
    return cmd


def kill_remote_process_by_name(remote_process_name, remote_user, remote_host, kill_args):
    run_remote_command_sync(kill_remote_process_by_name_cmd(remote_process_name,
                                                            kill_args), remote_user, remote_host)


def kill_remote_process_by_port_cmd(port, kill_args):
    return 'lsof -ti:%d | xargs kill%s' % (port, kill_args)


def kill_remote_process_by_port(port, remote_user, remote_host, kill_args):
    run_remote_command_sync(kill_remote_process_by_port_cmd(port, kill_args),
                            remote_user, remote_host)


def kill_process_by_name(process_name, kill_args):
    run_local_command_sync('pkill%s %s' % (kill_args, process_name))


def kill_process_by_port(port, kill_args):
    subprocess.run('lsof -ti:%d | xargs kill%s' % (port, kill_args),
                   stdout=subprocess.PIPE, universal_newlines=True, shell=True)


def get_timestamped_exp_dir(config):
    now_string = time.strftime('%Y-%m-%d-%H-%M-%S',
                               time.localtime())
    return os.path.join(config['base_local_exp_directory'], now_string)


def get_interface_for_ip(ip, remote_user, remote_host):
    return run_remote_command_sync('ifconfig | grep -B1 "inet addr:%s" | awk \'$1!="inet" && $1!="--" {print $1}\'' % ip, remote_user, remote_host).rstrip()


def get_exp_net_interface(remote_user, remote_host):
    return run_remote_command_sync('cat /var/emulab/boot/ifmap | awk \'{ print $1 }\'', remote_user, remote_host).rstrip()


def get_ip_for_server_name(server_name, remote_user, remote_host):
    return run_remote_command_sync('getent hosts %s | awk \'{ print $1 }\'' % server_name, remote_user, remote_host).rstrip()


def remove_delays(remote_user, remote_host):
    iface = get_exp_net_interface(remote_user, remote_host)
    run_remote_command_sync('sudo tc qdisc del dev %s root' %
                            iface, remote_user, remote_host)


def get_iface_add_delays(ip_to_delay, max_bandwidth, remote_user, remote_host):
    iface = get_exp_net_interface(remote_user, remote_host)
    add_delays_for_ips(ip_to_delay, iface, max_bandwidth, remote_user,
                       remote_host)


def add_delays_for_ips(ip_to_delay, interface, max_bandwidth, remote_user, remote_host):
    command = 'sudo tc qdisc del dev %s root; ' % interface
    command += 'sudo tc qdisc add dev %s root handle 1: htb; ' % interface
    command += 'sudo tc class add dev %s parent 1: classid 1:1 htb rate %s; ' % (
        interface, max_bandwidth)  # we want unlimited bandwidth
    idx = 2
    for ip, delay in ip_to_delay.items():
        command += 'sudo tc class add dev %s parent 1:1 classid 1:%d htb rate %s; ' % (
            interface, idx, max_bandwidth)
        command += 'sudo tc qdisc add dev %s handle %d: parent 1:%d netem delay %dms; ' % (
            interface, idx, idx, delay / 2)
        command += 'sudo tc filter add dev %s pref %d protocol ip u32 match ip dst %s flowid 1:%d; ' % (
            interface, idx, ip, idx)
        idx += 1
    run_remote_command_sync(command, remote_user, remote_host)


def get_name_to_ip_map(config, remote_user, remote_host):
    name_to_ip = {}
    for i in range(len(config['server_names'])):
        ip = get_ip_for_server_name(config['server_names'][i], remote_user,
                                    remote_host)
        name_to_ip[config['server_names'][i]] = ip

    for i in range(len(config['clients'])):
        client_name = config['clients'][i]
        ip = get_ip_for_server_name(client_name, remote_user, remote_host)
        name_to_ip[client_name] = ip

    return name_to_ip


def get_ip_to_delay(config, name_to_ip, server_name, delay_to_clients=False):
    ip_to_delay = {}
    region = None
    for reg, servers in config['server_regions'].items():
        if server_name in servers:
            region = reg
            break
    if region == None:
        raise Exception
    for reg, delay in config['region_rtt_latencies'][region].items():
        if reg != region and reg in config['server_regions']:
            for name in config['server_regions'][reg]:
                if name in config['server_names']:
                    ip_to_delay[name_to_ip[name]] = delay
    if delay_to_clients:
        for reg, delay in config['region_rtt_latencies'][region].items():
            if reg != region and reg in config['server_regions']:
                for name in config['server_regions'][reg]:
                    if name in config['clients']:
                        ip_to_delay[name_to_ip[name]] = delay

    return ip_to_delay
