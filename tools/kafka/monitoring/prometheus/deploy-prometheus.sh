#!/bin/bash

deploy()
{
    echo -e "deploy prometheus started\n"

	mkdir -p /data/prometheus
	chown -R deploy:deploy /data/prometheus

	mkdir -p /opt/ctrip/app2
	
	cd ~powerop
	mv prometheus-2.4.3.linux-amd64.tar.gz /opt/ctrip/app2
	mv prometheus.yml /opt/ctrip/app2
	mv prometheus.service /opt/ctrip/app2

	cd /opt/ctrip/app2
	
	tar -xf prometheus-2.4.3.linux-amd64.tar.gz && mv prometheus-2.4.3.linux-amd64 prometheus
	cp -f prometheus.yml prometheus/
	chown -R deploy:deploy /opt/ctrip/app2

	cp prometheus.service /etc/systemd/system/
	systemctl daemon-reload
	systemctl start prometheus
	systemctl enable prometheus

	rm ~powerop/deploy-prometheus.sh

    echo -e "\ndeploy prometheus finished"
}

deploy
