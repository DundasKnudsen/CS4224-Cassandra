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

echo "Spawn NC / 5 process ..."
 
for i in $(seq 1 $num_clients)
do
    if [ $((i%5 + 1)) == $node_id ]
    then
        log_file=$log_dir/$num_clients-$consistency_level-stats.txt
        touch $log_file
        python ./script/main.py $xact_dir/$i.txt $i $consistency_level $log_file &> $log_dir/$num_clients-$consistency_level/xact-$i.txt &
    fi
done

echo "Join NC / 5 process"

wait

echo "Sumarize..."

python ./script/summary.py $log_dir/$num_clients-$consistency_level-stats.txt > $log_dir/$num_clients-$consistency_level-summary.txt

echo "done on node $node_id with num_clients $num_clients and consistency_level $consistency_level"

