#!/bin/bash

mkdir -p /opt/data/zookeeper
mkdir -p /opt/logs/zookeeper
mkdir -p /opt/ctrip/app/zookeeper

cd /opt/ctrip/app/zookeeper

files[0]=zookeeper-3.4.13.tar.gz
files[1]=zoo.cfg
files[2]=log4j.properties
files[3]=myid
files[4]=zookeeper.service
for i in ${files[*]}
do
    mv ~powerop/$i ./
done

tar xf zookeeper-3.4.13.tar.gz
mv zookeeper-3.4.13 zookeeper
cp -f zoo.cfg zookeeper/conf/
cp -f log4j.properties zookeeper/conf/
cp -f myid /opt/data/zookeeper/

chown -R deploy:deploy /opt/data/zookeeper
chown -R deploy:deploy /opt/logs/zookeeper
chown -R deploy:deploy /opt/ctrip/app/zookeeper

cp -f zookeeper.service /etc/systemd/system/
systemctl daemon-reload

systemctl start zookeeper
systemctl enable zookeeper
systemctl status zookeeper

ps aux | grep zookeeper

rm ~powerop/deploy-zookeeper-service.sh
