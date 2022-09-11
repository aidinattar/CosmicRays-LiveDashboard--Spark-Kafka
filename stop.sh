#!/bin/bash

# Stop Spark Cluster
sh /usr/local/spark/sbin/stop-all.sh

sleep 1

# Stop Zookeeper
sh /usr/local/kafka/bin/zookeeper-server-stop.sh

sleep 1

# Stop Kafka
sh /usr/local/kafka/bin/kafka-server-stop.sh