#!/bin/bash

rm -r tmp/kafka-logs

#for run in {1..10}; do
#  lsof -ti:2181 | xargs kill -9
#done

# Start Spark Cluster
/usr/local/spark/sbin/start-all.sh

sleep 1

# Start Zookeeper
/usr/local/kafka/bin/zookeeper-server-start.sh /usr/local/kafka/config/zookeeper.properties &

sleep 1

# Start Kafka
/usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties &
