#!/usr/bin/env bash

# required variables:
# -   num_clients: number of clients to run
# -   consistency_level: consistency level

num_clients=$1
consistency_level=$2

log_dir=$(pwd)/log

echo "Summarize..."

python ./script/summary.py $log_dir/$num_clients-$consistency_level-stats.txt > $log_dir/$num_clients-$consistency_level-summary.txt

echo "Done."

