#!/bin/bash

start=0
end=3

for osd in `seq $start $end`
do
	echo mkdir -p $CEPH_DEV_DIR/osd$osd/db
	echo sudo mkfs.ext4 -f /dev/nvme`expr $osd + 3`n1
	echo sudo mount /dev/nvme`expr $osd + 3`n1 $CEPH_DEV_DIR/osd$osd/db
done
