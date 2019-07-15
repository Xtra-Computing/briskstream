#!/bin/bash
rm -fr ~/storm-local/*
rm ~/Documents/apache-storm-1.0.0/logs/*
numactl  --cpunodebind=0  --membind=0 ~/Documents/zookeeper-3.4.8/bin/zkServer.sh start
numactl  --cpunodebind=0  --membind=0 ~/Documents/apache-storm-1.0.0/bin/storm nimbus
