package com.ctrip.framework.kbear.client;

import org.apache.kafka.clients.producer.Producer;
import org.mydotey.scf.ConfigurationManager;

/**
 * @author koqizhao
 *
 * Jan 2, 2019
 */
public class CProducerProxy<K, V> extends ProducerProxy<K, V> {

    public CProducerProxy(ConfigurationManager configurationManager, KafkaMetaManager metaManager,
            KafkaProducerConfig<K, V> kafkaProducerConfig) {
        super(configurationManager, metaManager, kafkaProducerConfig);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected Producer newProducer(String topicId, KafkaProducerConfig kafkaProducerConfig) {
        Producer producer = super.newProducer(topicId, kafkaProducerConfig);
        return CatProxy.newInstance(producer, Producer.class, topicId);
    }

}
