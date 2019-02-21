#!/bin/bash

KM_PATH=/opt/ctrip/app/kafka-manager
mkdir -p $KM_PATH

cd $KM_PATH

files[0]=kafka-manager-1.3.3.18.zip
files[1]=restart.sh
files[2]=application.conf
for i in ${files[*]}
do
    mv -f ~powerop/$i ./
done

unzip kafka-manager-1.3.3.18.zip
mv kafka-manager-1.3.3.18 kafka-manager

chmod +x *.sh
cp -f application.conf kafka-manager/conf/

chown -R deploy:deploy $KM_PATH

su - deploy -c "cd $KM_PATH; ./restart.sh"
crontab -u deploy ~powerop/crontab.txt

rm ~powerop/deploy-kafka-manager.sh
rm ~powerop/crontab.txt

ps aux | grep kafka-manager
