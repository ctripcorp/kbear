#!/bin/bash

deploy()
{
    echo -e "deploy grafana started\n"

	mkdir -p /data/grafana
	mkdir -p /data/grafana/data
	mkdir -p /data/grafana/plugins
	chown -R deploy:deploy /data/grafana
	
	mkdir -p /opt/logs/grafana
	chown -R deploy:deploy /opt/logs/grafana

	mkdir -p /opt/ctrip/app2

	cd ~powerop
	mv grafana-5.2.4.linux-amd64.tar.gz /opt/ctrip/app2
	mv custom.ini /opt/ctrip/app2
	mv grafana.service /opt/ctrip/app2

	cd /opt/ctrip/app2
	
	tar -xf grafana-5.2.4.linux-amd64.tar.gz && mv grafana-5.2.4 grafana
	cp -f custom.ini grafana/conf/
	chown -R deploy:deploy /opt/ctrip/app2

	cp grafana.service /etc/systemd/system/
	systemctl daemon-reload
	systemctl start grafana
	systemctl enable grafana

	rm ~powerop/deploy-grafana.sh

    echo -e "\ndeploy grafana finished"
}

deploy
