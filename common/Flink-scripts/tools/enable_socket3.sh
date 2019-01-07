#/bin/bash
#socket3 : 3 7 11 15 19 23 27 31
echo 1 | sudo tee /sys/devices/system/cpu/cpu3/online
echo 1 | sudo tee /sys/devices/system/cpu/cpu7/online
echo 1 | sudo tee /sys/devices/system/cpu/cpu11/online
echo 1 | sudo tee /sys/devices/system/cpu/cpu15/online
echo 1 | sudo tee /sys/devices/system/cpu/cpu19/online
echo 1 | sudo tee /sys/devices/system/cpu/cpu23/online
echo 1 | sudo tee /sys/devices/system/cpu/cpu27/online
echo 1 | sudo tee /sys/devices/system/cpu/cpu31/online

