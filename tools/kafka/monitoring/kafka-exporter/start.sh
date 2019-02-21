#!/bin/bash

cd /opt/ctrip/app2/kafka-exporter
./kafka_exporter --kafka.server=<ip>:9092 > kafka_exporter.out
