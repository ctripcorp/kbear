pid=$(/usr/java/default/bin/jps -l | awk '$2=="kafka.Kafka"{print $1}')

if [ "${pid}" == "" ]; then
	cd /opt/ctrip/app/kafka/
	./kafka.sh daemon-start
fi
