package com.ctrip.framework.kbear.client;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.mydotey.java.ObjectExtension;
import org.mydotey.scf.ConfigurationManager;

/**
 * @author koqizhao
 *
 * Dec 18, 2018
 */
public class DefaultKafkaClientFactory implements KafkaClientFactory {

    private ConfigurationManager _configurationManager;
    private KafkaMetaManager _metaManager;

    public DefaultKafkaClientFactory(ConfigurationManager configurationManager, KafkaMetaManager metaManager) {
        ObjectExtension.requireNonNull(configurationManager, "configurationManager");
        ObjectExtension.requireNonNull(metaManager, "metaManager");
        _configurationManager = configurationManager;
        _metaManager = metaManager;
    }

    protected ConfigurationManager getConfigurationManager() {
        return _configurationManager;
    }

    protected KafkaMetaManager getMetaManager() {
        return _metaManager;
    }

    @Override
    public <K, V> Consumer<K, V> newConsumer(Map<String, Object> configs) {
        return newConsumer(configs, null, null);
    }

    @Override
    public <K, V> Consumer<K, V> newConsumer(Map<String, Object> configs, Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {
        ObjectExtension.requireNonNull(configs, "configs");
        Properties properties = new Properties();
        properties.putAll(configs);
        return newConsumer(properties, keyDeserializer, valueDeserializer);
    }

    @Override
    public <K, V> Consumer<K, V> newConsumer(Properties properties) {
        return newConsumer(properties, null, null);
    }

    @Override
    public <K, V> Consumer<K, V> newConsumer(Properties properties, Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {
        ObjectExtension.requireNonNull(properties, "properties");
        KafkaConsumerConfig<K, V> kafkaConsumerConfig = new KafkaConsumerConfig.Builder<K, V>()
                .setProperties(properties).setKeyDeserializer(keyDeserializer).setValueDeserializer(valueDeserializer)
                .build();
        return newConsumer(kafkaConsumerConfig);
    }

    @Override
    public <K, V> Consumer<K, V> newConsumer(KafkaConsumerConfig<K, V> kafkaConsumerConfig) {
        ObjectExtension.requireNonNull(kafkaConsumerConfig, "kafkaConsumerConfig");
        return new ConsumerProxy<>(_configurationManager, _metaManager, kafkaConsumerConfig.clone());
    }

    @Override
    public <K, V> Producer<K, V> newProducer(Map<String, Object> configs) {
        return newProducer(configs, null, null);
    }

    @Override
    public <K, V> Producer<K, V> newProducer(Map<String, Object> configs, Serializer<K> keySerializer,
            Serializer<V> valueSerializer) {
        ObjectExtension.requireNonNull(configs, "configs");
        Properties properties = new Properties();
        properties.putAll(configs);
        return newProducer(properties, keySerializer, valueSerializer);
    }

    @Override
    public <K, V> Producer<K, V> newProducer(Properties properties) {
        return newProducer(properties, null, null);
    }

    @Override
    public <K, V> Producer<K, V> newProducer(Properties properties, Serializer<K> keySerializer,
            Serializer<V> valueSerializer) {
        ObjectExtension.requireNonNull(properties, "properties");
        KafkaProducerConfig<K, V> kafkaProducerConfig = new KafkaProducerConfig.Builder<K, V>()
                .setProperties(properties).setKeySerializer(keySerializer).setValueSerializer(valueSerializer).build();
        return newProducer(kafkaProducerConfig);
    }

    @Override
    public <K, V> Producer<K, V> newProducer(KafkaProducerConfig<K, V> kafkaProducerConfig) {
        ObjectExtension.requireNonNull(kafkaProducerConfig, "kafkaProducerConfig");
        return new ProducerProxy<>(_configurationManager, _metaManager, kafkaProducerConfig.clone());
    }

    @Override
    public void close() throws Exception {
        _metaManager.close();
    }

}
