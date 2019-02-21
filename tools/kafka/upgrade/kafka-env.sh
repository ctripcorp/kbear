#!/bin/bash

export CLASSPATH=$CLASSPATH:custom-cp/:custom-libs/*
export LOG_DIR=/opt/logs/kafka
export KAFKA_HEAP_OPTS="-Xms8g -Xmx8g"
export KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:PermSize=128m -XX:MaxPermSize=128m -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -Djava.awt.headless=true -Djava.rmi.server.hostname=10.26.190.167"
export JMX_PORT=8302

