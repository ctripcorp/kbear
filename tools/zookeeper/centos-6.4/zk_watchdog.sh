pid=$(/usr/java/default/bin/jps -l | awk '$2=="org.apache.zookeeper.server.quorum.QuorumPeerMain"{print $1}')

if [ "${pid}" == "" ]; then
	cd /opt/ctrip/app/zookeeper-3.4.6
	bin/zkServer.sh start
fi
