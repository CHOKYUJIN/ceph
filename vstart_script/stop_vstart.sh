#!/bin/bash

sudo ../src/stop.sh

docker stop `docker ps | awk '{ print $1 }'`

sudo umount ./dev/osd*
rm -rf ./dev ./out
