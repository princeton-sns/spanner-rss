# Spanner-RSS

## What is Spanner-RSS?

Spanner-RSS is a variant of Spanner, Google's globally distributed database,
that relaxes its consistency from strict serializability to regular sequential
consistency. In doing so, Spanner-RSS is able to offer lower tail latency than
Spanner for read-only transactions. This code was used for the SOSP 2021 paper,
["Regular Sequential Serializability and Regular Sequential
Consistency."](https://dl.acm.org/doi/10.1145/3477132.3483566) It is based off
of the code originally used in the evaluation of
[TAPIR](https://dl.acm.org/doi/10.1145/2815400.2815404) from SOSP 2015.

This repository includes an implementation of Spanner's protocol (as described in
[Google's 2013 TOCS paper](https://dl.acm.org/doi/abs/10.1145/2491245)), an
implementation of our Spanner-RSS variant, and scripts to run the experiments
presented in our paper.

## Compiling & Running

### Tool Versions

We've built and run the spanner-rss with the following compiler tools:
* cmake v3.10.2
* gcc/g++ v7.5.0
* python v3.6.9
* gnuplot v5.2

### Running experiments

Experiments for the paper were run on CloudLab using the
[spanner-rss](https://www.cloudlab.us/p/cops/spanner-rss) profile.
After starting an experiment,

1. Clone the experiment repository to one of the CloudLab machines. (We often use `client-0-0`.)  
   `$ git clone --recursive https://github.com/princeton-sns/spanner-rss.git`

2. Build the C++ benchmark and server.  
   `$ cd spanner-rss/src/ && mkdir build && cd build`  
   `$ cmake .. && make`

3. Install experiment script dependencies.  
   `$ sudo apt update && sudo apt install -y python3-numpy gnuplot`

4. Update experiment config. You will likely need to update the following fields:
   - `project_name`
   - `experiment_name`
   - `base_local_exp_directory`
   - `base_remote_bin_directory_nfs`
   - `src_directory`
   - `src_commit_hash`

5. After updating the config file, you can run the experiment using a python3 script. For example,  
   `$ python3 ./experiments/run_multiple_experiments.py experiments/configs/retwis-wan-tput-lat-zipf-0.7.json`

## Authors
Jeffrey Helt, Amit Levy, Wyatt Lloyd -- Princeton University

Matthew Burke -- Cornell University
