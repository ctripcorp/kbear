#!/bin/bash

zk_path=/opt/ctrip/app/zookeeper

cd $zk_path

mv ~powerop/log4j.properties ./

cp -f log4j.properties zookeeper/conf/

chown -R deploy:deploy $zk_path

systemctl restart zookeeper.service

rm ~powerop/deploy-zookeeper-service.sh

ps aux | grep zookeeper
