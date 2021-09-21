import utils
import sys
import concurrent.futures

from utils.experiment_util import *

def main():
    if len(sys.argv) != 3:
        sys.stderr.write('Usage: python3 %s <config_file> <exp_dir>\n' % sys.argv[0])
        sys.exit(1)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        regenerate_plots(sys.argv[1], sys.argv[2], executor)


if __name__ == "__main__":
    main()
