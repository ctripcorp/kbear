#!/bin/bash

for i in `seq 1 12`
do
    echo "change /data$i/kafka owner to deploy"
    cd /data$i
    chown -R deploy:deploy kafka
    echo
done

echo "change /opt/logs/kafka owner to deploy"
cd /opt/logs
chown -R deploy:deploy kafka
echo

