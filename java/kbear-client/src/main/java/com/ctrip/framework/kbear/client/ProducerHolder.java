package com.ctrip.framework.kbear.client;

import org.apache.kafka.clients.producer.Producer;

import com.ctrip.framework.kbear.route.Route;

/**
 * @author koqizhao
 *
 * Dec 20, 2018
 */
public class ProducerHolder<K, V> {

    private String _topicId;
    private KafkaProducerConfig<K, V> _config;
    private Route _route;
    private Producer<K, V> _producer;

    public ProducerHolder(String topicId, KafkaProducerConfig<K, V> config, Route route, Producer<K, V> producer) {
        _topicId = topicId;
        _config = config;
        _route = route;
        _producer = producer;
    }

    public String getTopicId() {
        return _topicId;
    }

    public KafkaProducerConfig<K, V> getConfig() {
        return _config;
    }

    public Route getRoute() {
        return _route;
    }

    public Producer<K, V> getProducer() {
        return _producer;
    }

    @Override
    public String toString() {
        return "ProducerHolder [topicId=" + _topicId + ", config=" + _config + ", route=" + _route + "]";
    }

}
