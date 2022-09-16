#!/bin/bash

# Stop Spark Cluster
/usr/local/spark/sbin/stop-all.sh

sleep 1

# Stop Zookeeper
#/usr/local/kafka/bin/zookeeper-server-stop.sh
ssh pd-slave3 sudo -S systemctl stop zookeeper

sleep 1

# Stop Kafka
#/usr/local/kafka/bin/kafka-server-stop.sh
ssh pd-slave3 sudo -S systemctl stop kafka
