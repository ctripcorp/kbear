#!/bin/bash

deploy()
{
    echo -e "deploy kafka-exporter started\n"

	mkdir -p /opt/ctrip/app2/kafka-exporter

	cd ~powerop
	mv kafka_exporter /opt/ctrip/app2/kafka-exporter
	mv kafka-exporter.service /opt/ctrip/app2/kafka-exporter
	mv start.sh /opt/ctrip/app2/kafka-exporter

	chown -R deploy:deploy /opt/ctrip/app2

	cd /opt/ctrip/app2/kafka-exporter
	chmod +x kafka_exporter
	chmod +x start.sh
	cp kafka-exporter.service /etc/systemd/system/
	systemctl daemon-reload
	systemctl start kafka-exporter
	systemctl enable kafka-exporter

	rm ~powerop/deploy-kafka-exporter.sh

    echo -e "\ndeploy kafka-exporter finished"
}

deploy
