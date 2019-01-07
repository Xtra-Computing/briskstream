#/bin/bash
#socket0 : 0 4 8 12 16 21 24 28
echo 1 | sudo tee /sys/devices/system/cpu/cpu0/online
echo 1 | sudo tee /sys/devices/system/cpu/cpu4/online
echo 1 | sudo tee /sys/devices/system/cpu/cpu8/online
echo 1 | sudo tee /sys/devices/system/cpu/cpu12/online
echo 1 | sudo tee /sys/devices/system/cpu/cpu16/online
echo 1 | sudo tee /sys/devices/system/cpu/cpu21/online
echo 1 | sudo tee /sys/devices/system/cpu/cpu24/online
echo 1 | sudo tee /sys/devices/system/cpu/cpu28/online
