package com.ctrip.framework.kbear.client;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serializer;
import org.mydotey.java.ObjectExtension;
import org.mydotey.java.collection.PropertiesExtension;

/**
 * @author koqizhao
 *
 * Jan 2, 2019
 */
public class KafkaProducerConfig<K, V> implements Cloneable {

    private Properties properties;
    private Serializer<K> keySerializer;
    private Serializer<V> valueSerializer;

    protected KafkaProducerConfig() {

    }

    public Properties getProperties() {
        return properties;
    }

    public Serializer<K> getKeySerializer() {
        return keySerializer;
    }

    public Serializer<V> getValueSerializer() {
        return valueSerializer;
    }

    @SuppressWarnings("unchecked")
    @Override
    public KafkaProducerConfig<K, V> clone() {
        try {
            KafkaProducerConfig<K, V> obj = (KafkaProducerConfig<K, V>) super.clone();
            obj.properties = PropertiesExtension.clone(properties);
            return obj;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "KafkaProducerConfig [properties=" + properties + ", keySerializer=" + keySerializer
                + ", valueSerializer=" + valueSerializer + "]";
    }

    public static class Builder<K, V> {

        private KafkaProducerConfig<K, V> config;

        public Builder() {
            config = new KafkaProducerConfig<>();
        }

        public Builder<K, V> setProperties(Properties properties) {
            config.properties = properties;
            return this;
        }

        public Builder<K, V> setKeySerializer(Serializer<K> keySerializer) {
            config.keySerializer = keySerializer;
            return this;
        }

        public Builder<K, V> setValueSerializer(Serializer<V> valueSerializer) {
            config.valueSerializer = valueSerializer;
            return this;
        }

        public KafkaProducerConfig<K, V> build() {
            ObjectExtension.requireNonNull(config.properties, "properties");
            return config.clone();
        }

    }

}
