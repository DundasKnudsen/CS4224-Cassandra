#!/usr/bin/env bash

# required variables:
# -   node_id: id of this server, from 1-5
# -   num_clients: number of clients to run
# -   consistency_level: consistency level

echo "loading...."
bash load_data.sh $(pwd)/data/data-files

echo "running xact..."
bash main.sh $1 $2 $3 > $(pwd)/log/$1-$2-$3-log.txt
