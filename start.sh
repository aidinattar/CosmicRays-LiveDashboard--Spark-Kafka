#!/bin/bash

rm -r tmp/kafka-logs

# Start Spark Cluster
sh /usr/local/spark/sbin/start-all.sh --host localhost --port 7077 --webui-port 8080

sleep 1

# Start Zookeeper
sh /usr/local/kafka/bin/zookeeper-server-start.sh /usr/local/kafka/config/zookeeper.properties &

sleep 1

# Start Kafka
sh /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties &
