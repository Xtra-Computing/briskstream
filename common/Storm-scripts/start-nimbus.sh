#!/bin/bash
rm -rf ~/storm-local/*
rm -rf logs/*
numactl --localalloc -N 1 ~/Documents/zookeeper-3.5.0-alpha/bin/zkServer.sh start
numactl --localalloc -N 1 ~/Documents/apache-storm-1.1.1/bin/storm nimbus
