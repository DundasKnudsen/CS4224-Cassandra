#!/usr/bin/env bash

# required variables:
# -   node_id: id of this server, from 1-5
# -   num_clients: number of clients to run
# -   consistency_level: consistency level

node_id=$1
num_clients=$2
consistency_level=$3

xact_dir=$(pwd)/data/xact-files
log_dir=$(pwd)/log

mkdir -p log/$num_clients-$consistency_level

for i in $(seq 1 $num_clients)
do
    if [ $((i%5)) == $node_id ]
    then
        python ./script/main.py xact_dir/$i.txt $i $consistency_level $log_dir/$num_clients-$consistency_level-stats.txt &> $log_dir/$num_clients-$consistency_level/xact-$i.txt &
    fi
done

wait

python ./script/summary.py $log_dir/$num_clients-$consistency_level-stats.txt > $log_dir/$num_clients-$consistency_level-summary.txt

echo "done on node $node_id with num_clients $num_clients and consistency_level $consistency_level"

