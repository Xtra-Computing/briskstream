#!/bin/bash
numactl --localalloc -N 1 ~/Documents/apache-storm-1.1.1/bin/storm supervisor
