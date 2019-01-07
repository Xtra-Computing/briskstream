#!/bin/bash
SPARK_HOME=$HOME/Documents/spark-2.0.0-bin-hadoop2.7
$SPARK_HOME/sbin/stop-all.sh

config=$SPARK_HOME/conf/spark-defaults.conf
exe=$SPARK_HOME/sbin/start-slave.sh
$SPARK_HOME/sbin/start-all.sh
sleep 15

