#/bin/bash
#socket1 : 1 5 9 13 17 21 25 29
echo 0 | sudo tee /sys/devices/system/cpu/cpu1/online
echo 0 | sudo tee /sys/devices/system/cpu/cpu5/online
echo 0 | sudo tee /sys/devices/system/cpu/cpu9/online
echo 0 | sudo tee /sys/devices/system/cpu/cpu13/online
echo 0 | sudo tee /sys/devices/system/cpu/cpu17/online
echo 0 | sudo tee /sys/devices/system/cpu/cpu21/online
echo 0 | sudo tee /sys/devices/system/cpu/cpu25/online
echo 0 | sudo tee /sys/devices/system/cpu/cpu29/online
