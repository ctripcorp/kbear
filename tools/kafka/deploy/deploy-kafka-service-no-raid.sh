#!/bin/bash

super_user=${super_user}
run_user=${run_user}

mkdir -p /opt/app
mkdir -p /opt/data
mkdir -p /opt/logs/kafka
mkdir -p /opt/ctrip/app
chown -R ${run_user}:${run_user} /opt/app
chown -R ${run_user}:${run_user} /opt/data
chown -R ${run_user}:${run_user} /opt/logs
chown -R ${run_user}:${run_user} /opt/ctrip

for i in `seq 1 12`
do
    mkdir -p /data$i/kafka
    chown -R ${run_user}:${run_user} /data$i/kafka
done

cd /opt/ctrip/app

files[0]=kafka_2.11-2.0.0.tgz
files[1]=kafka-env.sh
files[2]=kafka.sh
files[3]=kafka.service
files[4]=server.properties
files[5]=log4j.properties
files[6]=ctrip-cp
files[7]=ctrip-libs
for i in ${files[*]}
do
    mv ~${super_user}/$i ./
done

tar xf kafka_2.11-2.0.0.tgz
mv kafka_2.11-2.0.0 kafka

chmod +x *.sh
cp kafka-env.sh kafka/
cp kafka.sh kafka/

cp -f server.properties kafka/config/
cp -f log4j.properties kafka/config/
cp -r ctrip-cp kafka/
cp -r ctrip-libs kafka/

chown -R ${run_user}:${run_user} *

cp kafka.service /etc/systemd/system/
systemctl daemon-reload

systemctl start kafka
systemctl enable kafka
systemctl status kafka

ps aux | grep kafka

rm ~${super_user}/deploy-kafka-service-no-raid.sh
