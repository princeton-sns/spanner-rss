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
Directions coming soon!

## Authors
Jeffrey Helt, Amit Levy, Wyatt Lloyd -- Princeton University

Matthew Burke -- Cornell University
