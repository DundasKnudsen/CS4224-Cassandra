# CS4224 Cassandra Project - Team 11

### Team members

- Nguyen Hung Tam	A0112059N
- Bui Do Hiep		A0126502U
- Adrian Bratteby A0175596L

### Setup
1. Download Cassandra into `/temp` folder
```sh
$ curl -o apache-cassandra-3.11.0-bin.tar.gz http://www-eu.apache.org/dist/cassandra/3.11.0/apache-cassandra-3.11.0-bin.tar.gz
```
2. Extract the file
```sh
$ tar -xvf apache-cassandra-3.11.0-bin.tar.gz
```
3. Copy config
For each node, copy corresponding `cassandra.yml` file into the `/temp/apache-cassandra-3.11.0/conf/` folder
4. Run cassandra
```sh
$ /temp/apache-cassandra-3.11.0/bin/cassandra
```
Remember to start the seed nodes before other nodes.

### Running Experiment
1. Load data
```sh
$ bash load_data.sh <data-files-absolute-path>
```
2. Run transaction
On node `node_id` with `node_id` from 1 to 5, we run the following command to start the clients
```sh
$ bash main.sh <node_id> <number_clients> <consistency_level> 
```
3. Summary 
```sh
$ bash summary.sh <number_clients> <consistency_level>
```

### Logging
Before running any experient, remember to create the log foler in the root project folder
```sh
$ mkdir log
```
After finishing any experiment, you can take a look at all the log file in the log folder
