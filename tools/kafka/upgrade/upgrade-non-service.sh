#!/bin/bash

su - deploy -c 'cd /opt/ctrip/app/kafka; ./kafka.sh stop'
echo "stop kafka complete"

cd /opt/ctrip/app/

cp -f server.properties server.properties.0.9.0
cp -f kafka-env.sh kafka-env.sh.0.9.0
mv -f kafka kafka.0.9.0
echo "back up complete"
echo 

mv ~powerop/kafka_2.11-2.0.0.tgz ./
tar xf kafka_2.11-2.0.0.tgz
mv kafka_2.11-2.0.0 kafka

mv ~powerop/ctrip-cp ./
mv ~powerop/ctrip-libs ./
mv ~powerop/log4j.properties ./
mv ~powerop/server.properties ./
mv ~powerop/kafka-env.sh ./

chmod +x *.sh

cp -f kafka-env.sh kafka/
cp -f kafka.sh kafka/
cp -f kafka-watchdog.sh kafka/
cp -f server.properties kafka/config/
cp -f log4j.properties kafka/config/
cp -rf ctrip-cp kafka/
cp -rf ctrip-libs kafka/

chown -R deploy:deploy *

echo "update files complete"
echo

chmod +r /opt/settings/server.properties

mkdir -p /opt/app
mkdir -p /opt/data
mkdir -p /opt/logs
chown -R deploy:deploy /opt/app
chown -R deploy:deploy /opt/data
chown -R deploy:deploy /opt/logs

echo "init opt dirs complete"

su - deploy -c 'cd /opt/ctrip/app/kafka; ./kafka.sh daemon-start'

echo "start kafka complete"
echo

rm ~powerop/upgrade-non-service.sh
