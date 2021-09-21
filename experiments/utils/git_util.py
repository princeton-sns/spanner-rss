import subprocess
import os
import shutil


def compile_make(config):
    e = os.environ.copy()
    if 'make_env' in config:
        for k, v in config['make_env'].items():
            e[k] = v
    build_path = os.path.join(config['src_directory'], 'build')
    if not 'make_clean' in config or config['make_clean']:
        subprocess.call(["rm", "-r", build_path])
    os.makedirs(build_path, exist_ok=True)
    subprocess.call(["cmake", ".."], cwd=build_path, env=e)
    subprocess.call(["make", "-j", "8"], cwd=build_path, env=e)
    bin_path = os.path.join(config['src_directory'], 'bin')
    os.makedirs(bin_path, exist_ok=True)
    if 'make_collect_bins' in config:
        for f in config['make_collect_bins']:
            shutil.copy2(os.path.join(build_path, f),
                         os.path.join(bin_path, os.path.basename(f)))
    return bin_path


def get_current_branch(src_directory, target="HEAD"):
    return subprocess.check_output(["git", "rev-parse", target],
                                   cwd=src_directory).decode('utf-8').rstrip()


def get_current_branch_short(src_directory):
    return subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"],
                                   cwd=src_directory).decode('utf-8').rstrip()


def checkout_commit(src_directory, src_commit_hash):
    subprocess.call(["git", "checkout", src_commit_hash],
                    cwd=src_directory)


def stash_changes(src_directory):
    subprocess.call(["git", "stash"], cwd=src_directory)


def pop_stashed_changes(src_directory):
    subprocess.call(["git", "stash", "pop"], cwd=src_directory)


def clean_working_directory(src_directory):
    return subprocess.run(['git', 'status', '--untracked-files=no', '--porcelain'], cwd=src_directory).returncode == 0


def remake_binaries(config):
    clean = clean_working_directory(config['src_directory'])
    if not clean:
        stash_changes(config['src_directory'])
    current_branch = get_current_branch(config['src_directory'])
    current_branch_short = get_current_branch_short(config['src_directory'])
    print(current_branch)
    checked_out = False
    if 'src_commit_hash' in config:
        target_branch = get_current_branch(
            config['src_directory'], config['src_commit_hash'])
        if target_branch != current_branch:
            checkout_commit(config['src_directory'], config['src_commit_hash'])
            checked_out = True
    else:
        config['src_commit_hash'] = current_branch
    compile_make(config)
    if checked_out:
        checkout_commit(config['src_directory'], current_branch_short)
    if not clean:
        pop_stashed_changes(config['src_directory'])
