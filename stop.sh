#!/bin/bash

# Stop Spark Cluster
./../spark-3.3.0-bin-hadoop3/sbin/stop-all.sh

sleep 1

# Stop Zookeeper
./../kafka_2.13-3.2.1/bin/zookeeper-server-stop.sh &

sleep 1

# Stop Kafka
./../kafka_2.13-3.2.1/bin/kafka-server-stop.sh &
