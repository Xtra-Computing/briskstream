#!/bin/bash
numactl -N 1 -m 1 stress -m 1 --vm-bytes 128G --vm-keep &
numactl -N 2 -m 2 stress -m 1 --vm-bytes 128G --vm-keep &
#numactl -N 3 -m 3 stress -m 1 --vm-bytes 128G --vm-keep &
