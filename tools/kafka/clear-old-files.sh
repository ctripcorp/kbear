#!/bin/bash

echo "remove kafka 0.9.0.1"
rm -rf /opt/ctrip/app/kafka_2.11-0.9.0.1
rm -rf /opt/ctrip/app/kafka_2.11-0.9.0.1.tar.gz
rm -rf /opt/ctrip/app/kafka_2.11-0.9.0.1.tgz

echo "clear logs"
cd /opt/logs/kafka/
for i in `seq 2015 2018`
do
	for j in 01 02 03 04 05 06 07 08 09 10 11 12
	do
		rm -rf *$i-$j*
	done
done

rm -rf kafkaServer-gc.log
rm -rf kafkaServer.out

ls -l /opt/ctrip/app
ls -l /opt/logs/kafka

rm -rf ~powerop/clear-old-files.sh
