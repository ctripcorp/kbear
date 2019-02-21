#!/bin/bash

cd /opt/ctrip/app/

echo "before clean"
ls
echo

rm kafka_2.11-0.9.0.2.tgz
rm server.properties.0.9.0
rm kafka-env.sh.0.9.0
rm -rf kafka.0.9.0

rm ~powerop/clean-legacy.sh

echo "after clean"
ls
echo
