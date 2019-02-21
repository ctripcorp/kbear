pid=$(/usr/java/default/bin/jps -l | awk '$2=="jmx_exporter.jar"{print $1}')

if [ "${pid}" == "" ]; then
	cd /opt/ctrip/app2/kafka-jmx-exporter/
	./start.sh daemon
fi
