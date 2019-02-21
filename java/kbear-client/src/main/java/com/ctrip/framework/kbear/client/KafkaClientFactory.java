package com.ctrip.framework.kbear.client;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * @author koqizhao
 *
 * Jan 2, 2019
 */
public interface KafkaClientFactory extends AutoCloseable {

    <K, V> Consumer<K, V> newConsumer(Map<String, Object> configs);

    <K, V> Consumer<K, V> newConsumer(Map<String, Object> configs, Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer);

    <K, V> Consumer<K, V> newConsumer(Properties properties);

    <K, V> Consumer<K, V> newConsumer(Properties properties, Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer);

    <K, V> Consumer<K, V> newConsumer(KafkaConsumerConfig<K, V> kafkaConsumerConfig);

    <K, V> Producer<K, V> newProducer(Map<String, Object> configs);

    <K, V> Producer<K, V> newProducer(Map<String, Object> configs, Serializer<K> keySerializer,
            Serializer<V> valueSerializer);

    <K, V> Producer<K, V> newProducer(Properties properties);

    <K, V> Producer<K, V> newProducer(Properties properties, Serializer<K> keySerializer,
            Serializer<V> valueSerializer);

    <K, V> Producer<K, V> newProducer(KafkaProducerConfig<K, V> kafkaProducerConfig);

}