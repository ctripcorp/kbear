package com.ctrip.framework.kbear.client;

import java.util.Properties;

import org.apache.kafka.common.serialization.Deserializer;
import org.mydotey.java.ObjectExtension;
import org.mydotey.java.collection.PropertiesExtension;

/**
 * @author koqizhao
 *
 * Jan 2, 2019
 */
public class KafkaConsumerConfig<K, V> implements Cloneable {

    private Properties properties;
    private Deserializer<K> keyDeserializer;
    private Deserializer<V> valueDeserializer;

    private ConsumerRestartListener<K, V> consumerRestartListener;

    protected KafkaConsumerConfig() {

    }

    public Properties getProperties() {
        return properties;
    }

    public Deserializer<K> getKeyDeserializer() {
        return keyDeserializer;
    }

    public Deserializer<V> getValueDeserializer() {
        return valueDeserializer;
    }

    public ConsumerRestartListener<K, V> getConsumerRestartListener() {
        return consumerRestartListener;
    }

    @SuppressWarnings("unchecked")
    @Override
    public KafkaConsumerConfig<K, V> clone() {
        try {
            KafkaConsumerConfig<K, V> obj = (KafkaConsumerConfig<K, V>) super.clone();
            obj.properties = PropertiesExtension.clone(properties);
            return obj;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "KafkaConsumerConfig [properties=" + properties + ", keyDeserializer=" + keyDeserializer
                + ", valueDeserializer=" + valueDeserializer + ", consumerRestartListener=" + consumerRestartListener
                + "]";
    }

    public static class Builder<K, V> {

        private KafkaConsumerConfig<K, V> config;

        public Builder() {
            config = new KafkaConsumerConfig<>();
        }

        public Builder<K, V> setProperties(Properties properties) {
            config.properties = properties;
            return this;
        }

        public Builder<K, V> setKeyDeserializer(Deserializer<K> keyDeserializer) {
            config.keyDeserializer = keyDeserializer;
            return this;
        }

        public Builder<K, V> setValueDeserializer(Deserializer<V> valueDeserializer) {
            config.valueDeserializer = valueDeserializer;
            return this;
        }

        public Builder<K, V> setConsumerRestartListener(ConsumerRestartListener<K, V> consumerRestartListener) {
            config.consumerRestartListener = consumerRestartListener;
            return this;
        }

        public KafkaConsumerConfig<K, V> build() {
            ObjectExtension.requireNonNull(config.properties, "properties");
            return config.clone();
        }

    }

}
