#!/bin/bash

KAFKA_PATH=/opt/ctrip/app/kafka
MIRROR_MAKER_PATH=/opt/ctrip/app2/kafka-mirror-maker

export LOG_DIR=/opt/logs/kafka-mirror-maker
export KAFKA_HEAP_OPTS="-Xms2g -Xmx2g"
export KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent -Djava.awt.headless=true"

cd $KAFKA_PATH
bin/kafka-mirror-maker.sh --num.streams 10 --consumer.config $MIRROR_MAKER_PATH/consumer.properties --producer.config $MIRROR_MAKER_PATH/producer.properties --whitelist "bbz.test.ubt.custom.created|bbz.test.ubt.usermetric.created" > $MIRROR_MAKER_PATH/mirror-maker.out 2>&1 &
