import utils
import sys
import concurrent.futures

from utils.experiment_util import *

def main():
    if len(sys.argv) != 2:
        sys.stderr.write('Usage: python3 %s <config_file>\n' % sys.argv[0])
        sys.exit(1)

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        print(run_experiment(sys.argv[1], 0, executor).result())


if __name__ == "__main__":
    main()
