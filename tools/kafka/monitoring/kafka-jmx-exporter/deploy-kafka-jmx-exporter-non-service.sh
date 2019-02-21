#!/bin/bash

deploy()
{
    echo -e "deploy kafka-jmx-exporter started\n"

	mkdir -p /opt/ctrip/app2/kafka-jmx-exporter

	cd ~powerop
	mv jmx_exporter.jar /opt/ctrip/app2/kafka-jmx-exporter
	mv kafka.yml /opt/ctrip/app2/kafka-jmx-exporter
	mv watchdog.sh /opt/ctrip/app2/kafka-jmx-exporter
	mv start.sh /opt/ctrip/app2/kafka-jmx-exporter

	chown -R deploy:deploy /opt/ctrip/app2

	cd /opt/ctrip/app2/kafka-jmx-exporter
	chmod +x *.sh

	rm ~powerop/deploy-kafka-jmx-exporter-non-service.sh

    echo -e "\ndeploy kafka-jmx-exporter finished"
}

deploy
