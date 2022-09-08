#!/bin/bash

# Start Spark Cluster
./../spark-3.3.0-bin-hadoop3/sbin/start-all.sh

sleep 1

# Start Zookeeper
./../kafka_2.13-3.2.1/bin/zookeeper-server-start.sh ../kafka_2.13-3.2.1/config/zookeeper.properties &

sleep 1

# Start Kafka
./../kafka_2.13-3.2.1/bin/kafka-server-start.sh ../kafka_2.13-3.2.1/config/server.properties &
