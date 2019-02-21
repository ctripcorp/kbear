package com.ctrip.framework.kbear.client;

import org.apache.kafka.clients.consumer.Consumer;
import org.mydotey.scf.ConfigurationManager;

import com.ctrip.framework.kbear.meta.ConsumerGroupId;

/**
 * @author koqizhao
 *
 * Jan 2, 2019
 */
public class CConsumerProxy<K, V> extends ConsumerProxy<K, V> {

    public CConsumerProxy(ConfigurationManager configurationManager, KafkaMetaManager metaManager,
            KafkaConsumerConfig<K, V> kafkaConsumerConfig) {
        super(configurationManager, metaManager, kafkaConsumerConfig);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected Consumer newConsumer(ConsumerGroupId consumerGroupId, KafkaConsumerConfig kafkaConsumerConfig) {
        Consumer consumer = super.newConsumer(consumerGroupId, kafkaConsumerConfig);
        return CatProxy.newInstance(consumer, Consumer.class,
                consumerGroupId.getTopicId() + ":" + consumerGroupId.getGroupName());
    }

}
