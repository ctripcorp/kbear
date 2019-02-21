#!/bin/bash

TIMEOUT_SEC=60
if [ "$2" != "" ]
then
    TIMEOUT_SEC=$2
fi

getKafkaPS()
{
    ps ax | grep 'kafka\.Kafka' | grep java | grep -v grep | awk '{print $1}'
}

startServer() {
    echo "config kafka env:"
    echo
    cat kafka-env.sh
    source kafka-env.sh
    echo

    echo "start kafka server ..."
    if [ "$1" = "daemon-start" ]
    then
        bin/kafka-server-start.sh -daemon config/server.properties
        echo "kafka started"
        echo
    else
        bin/kafka-server-start.sh config/server.properties
    fi
}

stopServer() {
    KAFKA_PS=`getKafkaPS`
    echo "kafka ps: $KAFKA_PS"
    echo

    if [ "$KAFKA_PS" != "" ]
    then
        echo "stop kafka server ..."
        echo
        kill -SIGTERM $KAFKA_PS

        SLEEP_TIME=0
        while [ "$KAFKA_PS" != "" ] && [ $SLEEP_TIME -lt $TIMEOUT_SEC ]
        do
            echo "sleep 5s for complete"
            echo
            sleep 5
            SLEEP_TIME=`expr $SLEEP_TIME + 5`

            KAFKA_PS=`getKafkaPS`
        done

        echo "total time: $SLEEP_TIME"
        echo

        if [ "$KAFKA_PS" != "" ]
        then
            echo "kafka has not yet been stopped, force kill!"
            echo
            kill -9 $KAFKA_PS
        fi
    fi

    echo "kafka server stopped"
    echo
}

case $1 in
    "start")
        startServer
        ;;
    "daemon-start")
        startServer daemon-start
        ;;
    "stop")
        stopServer
        ;;
    *)
        echo "do nothing"
        ;;
esac

