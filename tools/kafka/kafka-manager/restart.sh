#!/bin/bash

KM_PATH=/opt/ctrip/app/kafka-manager/kafka-manager
set +e
PID_FILE=$KM_PATH/RUNNING_PID
OP_LOG=/opt/ctrip/app/kafka-manager/kafka-manager/op.log
echo "`date`" >> $OP_LOG
for OLD_PID in `ps aux | grep kafka | grep manager |grep -v restart| awk '{print $2}'`
do
        echo "Killing old kafka manager:$OLD_PID" >> $OP_LOG
        kill -9 $OLD_PID
        echo "Killing result: $?" >> $OP_LOG
done

if [[ -f $PID_FILE ]]; then
        echo "Removing old PID file ..." >> $OP_LOG
        rm $PID_FILE
        echo "Removing result: $?" >> $OP_LOG
fi

echo "Starting new kafka-manager ..." >> $OP_LOG
ulimit -u 65535
$KM_PATH/bin/kafka-manager -Dconfig.file=$KM_PATH/conf/application.conf > $KM_PATH/sysout.log 2>&1 &
echo "New kafka-manager started: $?" >> $OP_LOG
set -e
