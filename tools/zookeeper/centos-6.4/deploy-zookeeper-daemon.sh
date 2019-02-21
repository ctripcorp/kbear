#!/bin/bash

zk_path=/opt/ctrip/app/zookeeper-3.4.6

cd $zk_path

mv ~powerop/zk_watchdog.sh bin/
mv ~powerop/log4j.properties conf/

chown -R deploy:deploy $zk_path
rm ~deploy/zookeeper.out

su - deploy -c 'cd /opt/ctrip/app/zookeeper-3.4.6; bin/zkServer.sh restart'

rm ~powerop/deploy-zookeeper-daemon.sh
