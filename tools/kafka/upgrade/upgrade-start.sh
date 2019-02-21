#!/bin/bash

su - deploy -c 'cd /opt/ctrip/app/kafka; ./kafka.sh daemon-start'

ps aux | grep kafka
