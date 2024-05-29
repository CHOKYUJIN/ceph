#!/bin/bash

OSD_DEV="/dev/nvme1n1,/dev/nvme2n1,/dev/nvme3n1"

#MON=1 MGR=1 OSD=1 RGW=1 MDS=0 ../src/vstart.sh -n --bluestore --bluestore-devs ${OSD_DEV}
#MON=1 MGR=1 OSD=1 RGW=1 MDS=0 ../src/vstart.sh --jaeger -n -X --bluestore --bluestore-devs ${OSD_DEV}
#MON=1 MGR=1 OSD=3 RGW=1 MDS=0 ../src/vstart.sh -n --bluestore --no-parallel 
# MON=1 MGR=1 OSD=3 RGW=1 MDS=0 ../src/vstart.sh -n --bluestore --bluestore-devs ${OSD_DEV}
MON=1 MGR=1 OSD=1 RGW=1 MDS=0 ../src/vstart.sh -n --bluestore
