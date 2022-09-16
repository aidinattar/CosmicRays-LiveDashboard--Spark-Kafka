#!/bin/bash

#rm -r tmp/kafka-logs

for run in {1..5}; do
  ssh pd-slave3 lsof -ti:2181 | xargs kill -9 # > /dev/null 2>&1
done

# Start Spark Cluster
/usr/local/spark/sbin/start-all.sh

sleep 1


# Start Zookeeper
#/usr/local/kafka/bin/zookeeper-server-start.sh /usr/local/kafka/config/zookeeper.properties &
ssh pd-slave3 sudo -S systemctl start zookeeper

sleep 1

# Start Kafka
#/usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties &
ssh pd-slave3 sudo -S systemctl start kafka
