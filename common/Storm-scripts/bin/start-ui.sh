#!/bin/bash
numactl --cpunodebind=0  --membind=0 ~/Documents/apache-storm-1.0.0/bin/storm ui
