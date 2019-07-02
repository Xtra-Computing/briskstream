#!/bin/bash
JAR_PATH="$HOME/NUMA-streamBenchmarks/common/target/common-1.0-SNAPSHOT-jar-with-dependencies.jar"
MAIN="utils"
KAFKA_HOME="$HOME/kafka"
cores="$(getconf _NPROCESSORS_ONLN)"
#Only spark need multiple partitions...
$KAFKA_HOME/bin/kafka-topics.sh --create --topic "mb" --replication 1 --zookeeper localhost:2181 --partitions 1
#'function', 'tuple_size', 'skew', 'do not test', `do not verbose`.
java -jar $JAR_PATH $MAIN 1 $1 $2 false $3
