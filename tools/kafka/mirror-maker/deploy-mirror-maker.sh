#!/bin/bash

MIRROR_MAKER_PATH=/opt/ctrip/app2/kafka-mirror-maker

deploy_dirs[0]=$MIRROR_MAKER_PATH
deploy_dirs[1]=/opt/logs/kafka-mirror-maker

for i in ${deploy_dirs[*]}
do
	mkdir -p $i
done

deploy_files[0]=consumer.properties
deploy_files[1]=producer.properties
deploy_files[2]=start-mirror-maker.sh
for i in ${deploy_files[*]}
do
	mv -f ~powerop/$i $MIRROR_MAKER_PATH
done

cd $MIRROR_MAKER_PATH
chmod +x *.sh

for i in ${deploy_dirs[*]}
do
	chown -R deploy:deploy $i
done

su - deploy -c "cd $MIRROR_MAKER_PATH; ./start-mirror-maker.sh"

ps aux | grep mirror-maker

rm ~powerop/deploy-mirror-maker.sh
