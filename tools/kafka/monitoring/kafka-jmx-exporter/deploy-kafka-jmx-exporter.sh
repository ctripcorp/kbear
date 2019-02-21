#!/bin/bash

deploy()
{
    echo -e "deploy kafka-jmx-exporter started\n"

	mkdir -p /opt/ctrip/app2/kafka-jmx-exporter

	cd ~powerop
	mv jmx_exporter.jar /opt/ctrip/app2/kafka-jmx-exporter
	mv kafka.yml /opt/ctrip/app2/kafka-jmx-exporter
	mv kafka-jmx-exporter.service /opt/ctrip/app2/kafka-jmx-exporter
	mv start.sh /opt/ctrip/app2/kafka-jmx-exporter

	chown -R deploy:deploy /opt/ctrip/app2

	cd /opt/ctrip/app2/kafka-jmx-exporter
	chmod +x start.sh
	cp kafka-jmx-exporter.service /etc/systemd/system/
	systemctl daemon-reload
	systemctl start kafka-jmx-exporter
	systemctl enable kafka-jmx-exporter

	rm ~powerop/deploy-kafka-jmx-exporter.sh

    echo -e "\ndeploy kafka-jmx-exporter finished"
}

deploy
