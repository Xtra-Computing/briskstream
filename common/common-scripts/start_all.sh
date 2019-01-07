#/bin/bash
rm -rf ~/kafka-logs/*
rm -rf ~/zookeeper/*
~/Documents/zookeeper-3.5.2-alpha/bin/zkServer.sh start
$HOME/kafka/bin/kafka-topics.sh --delete --topic "mb" --zookeeper localhost:2181
run_kafka.sh
