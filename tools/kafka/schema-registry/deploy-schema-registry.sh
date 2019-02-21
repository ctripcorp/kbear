#!/bin/bash

mkdir -p /opt/ctrip/app

cd /opt/ctrip/app

mv ~powerop/confluent.tar ./
tar xf confluent.tar

mv ~powerop/schema-registry.properties ./
cp -f schema-registry.properties confluent/etc/schema-registry/

chown -R deploy:deploy /opt/ctrip/app

rm ~powerop/deploy-schema-registry.sh
