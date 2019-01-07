#/bin/bash
#socket2 : 2 6 10 14 18 22 26 30
echo 1 | sudo tee /sys/devices/system/cpu/cpu2/online
echo 1 | sudo tee /sys/devices/system/cpu/cpu6/online
echo 1 | sudo tee /sys/devices/system/cpu/cpu10/online
echo 1 | sudo tee /sys/devices/system/cpu/cpu14/online
echo 1 | sudo tee /sys/devices/system/cpu/cpu18/online
echo 1 | sudo tee /sys/devices/system/cpu/cpu22/online
echo 1 | sudo tee /sys/devices/system/cpu/cpu26/online
echo 1 | sudo tee /sys/devices/system/cpu/cpu30/online

