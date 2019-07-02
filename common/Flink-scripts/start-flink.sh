#!/bin/bash
FLINK_HOME="$HOME/Documents/flink-1.3.2"
$FLINK_HOME/bin/stop-cluster.sh
sleep 5
$FLINK_HOME/bin/start-cluster.sh
sleep 15
