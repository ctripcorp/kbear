package com.ctrip.framework.kbear.client;

import java.util.Collection;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import com.ctrip.framework.kbear.meta.ConsumerGroupId;
import com.ctrip.framework.kbear.route.Route;

/**
 * @author koqizhao
 *
 * Dec 20, 2018
 */
public class ConsumerHolder<K, V> {

    private ConsumerGroupId _consumerGroupId;
    private KafkaConsumerConfig<K, V> _config;
    private Route _route;
    private Consumer<K, V> _consumer;
    private volatile ConsumerRebalanceListener _consumerRebalanceListener;
    private volatile Collection<TopicPartition> _assignments;

    public ConsumerHolder(ConsumerGroupId consumerGroupId, KafkaConsumerConfig<K, V> config, Route route,
            Consumer<K, V> consumer) {
        _consumerGroupId = consumerGroupId;
        _config = config;
        _route = route;
        _consumer = consumer;
    }

    public ConsumerGroupId getConsumerGroupId() {
        return _consumerGroupId;
    }

    public KafkaConsumerConfig<K, V> getConfig() {
        return _config;
    }

    public Route getRoute() {
        return _route;
    }

    public Consumer<K, V> getConsumer() {
        return _consumer;
    }

    public ConsumerRebalanceListener getConsumerRebalanceListener() {
        return _consumerRebalanceListener;
    }

    public void setConsumerRebalanceListener(ConsumerRebalanceListener consumerRebalanceListener) {
        _consumerRebalanceListener = consumerRebalanceListener;
    }

    public Collection<TopicPartition> getAssignments() {
        return _assignments;
    }

    public void setAssignments(Collection<TopicPartition> assignments) {
        _assignments = assignments;
    }

    @Override
    public String toString() {
        return "ConsumerHolder [consumerGroupId=" + _consumerGroupId + ", config=" + _config + ", route=" + _route
                + "]";
    }

}
