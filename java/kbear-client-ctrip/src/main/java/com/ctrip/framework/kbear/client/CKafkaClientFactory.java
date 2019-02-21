package com.ctrip.framework.kbear.client;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.mydotey.java.CloseableExtension;
import org.mydotey.java.ObjectExtension;
import org.mydotey.java.StringExtension;
import org.mydotey.scf.ConfigurationManager;
import org.mydotey.scf.ConfigurationManagerConfig;
import org.mydotey.scf.facade.ConfigurationManagers;
import org.mydotey.scf.facade.ConfigurationSources;
import org.mydotey.scf.facade.StringPropertySources;
import org.mydotey.scf.source.stringproperty.propertiesfile.PropertiesFileConfigurationSource;
import org.mydotey.scf.source.stringproperty.propertiesfile.PropertiesFileConfigurationSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.framework.foundation.Env;
import com.ctrip.framework.foundation.EnvFamily;
import com.ctrip.framework.foundation.Foundation;
import com.ctrip.framework.foundation.spi.provider.ApplicationProvider;
import com.ctrip.framework.foundation.spi.provider.NetworkProvider;
import com.ctrip.framework.foundation.spi.provider.ServerProvider;
import com.ctrip.framework.kbear.route.Client;

/**
 * @author koqizhao
 *
 * Jan 2, 2019
 */
public class CKafkaClientFactory extends DefaultKafkaClientFactory {

    private static Logger _logger = LoggerFactory.getLogger(CKafkaClientFactory.class);

    private static volatile KafkaClientFactory _default;

    protected static ConfigurationManager newDefaultConfigurationManager() {
        EnvConfigurationSource envSource = new EnvConfigurationSource(ConfigurationSources.newConfig("env"));
        PropertiesFileConfigurationSourceConfig localFileConfig = StringPropertySources
                .newPropertiesFileSourceConfigBuilder().setFileName("kafka").setName("local-properties").build();
        PropertiesFileConfigurationSource localFileSource = StringPropertySources
                .newPropertiesFileSource(localFileConfig);
        ConfigurationManagerConfig managerConfig = ConfigurationManagers.newConfigBuilder()
                .setName("kbear-kafka-client").addSource(0, envSource).addSource(1, localFileSource).build();
        return ConfigurationManagers.newManager(managerConfig);
    }

    public static Client newDefaultClient() {
        ApplicationProvider app = Foundation.app();
        String id = app == null ? null : app.getAppId();
        HashMap<String, String> meta = new HashMap<>();
        tryPut(meta, "appId", id);
        tryPut(meta, "type", "kbear");
        tryPut(meta, "lang", "java");

        NetworkProvider net = Foundation.net();
        if (net != null) {
            tryPut(meta, "ip", net.getHostAddress());
            tryPut(meta, "hostName", net.getHostName());
        }

        ServerProvider server = Foundation.server();
        if (server != null) {
            tryPut(meta, "idc", server.getDataCenter());
            tryPut(meta, "cluster", server.getClusterName());

            Env env = server.getEnv();
            if (env != null)
                tryPut(meta, "env", env.getName());

            EnvFamily envFamily = server.getEnvFamily();
            if (envFamily != null)
                tryPut(meta, "envFamily", envFamily.getName());

            tryPut(meta, "subEnv", server.getSubEnv());
        }

        Client client = new Client();
        client.setId(id);
        client.setMeta(meta);
        _logger.info("default client info: {}", client);
        return client;
    }

    private static void tryPut(HashMap<String, String> map, String key, String value) {
        if (!StringExtension.isBlank(value))
            map.put(key, value);
    }

    public static KafkaClientFactory getDefault() {
        if (_default != null)
            return _default;

        synchronized (CKafkaMetaManager.class) {
            if (_default != null)
                return _default;

            _default = new CKafkaClientFactory(newDefaultClient());
        }

        return _default;
    }

    private CMetricsReporter _metricsReporter;

    public CKafkaClientFactory(Client client) {
        this(newDefaultConfigurationManager(), client);
    }

    protected CKafkaClientFactory(ConfigurationManager configurationManager, Client client) {
        super(configurationManager, new CKafkaMetaManager(configurationManager, client));

        _metricsReporter = new CMetricsReporter(toMetricTags(client));
    }

    protected Map<String, String> toMetricTags(Client client) {
        Map<String, String> metricTags = new HashMap<>();
        if (client.getMeta() != null) {
            metricTags.putAll(client.getMeta());
            metricTags.remove("ip");
            metricTags.remove("hostName");
            metricTags.remove("envFamily");
            metricTags.remove("cluster");
            metricTags.remove("env");
            metricTags.remove("subEnv");
        }
        return metricTags;
    }

    @Override
    protected CKafkaMetaManager getMetaManager() {
        return (CKafkaMetaManager) super.getMetaManager();
    }

    @SuppressWarnings("resource")
    @Override
    public <K, V> Consumer<K, V> newConsumer(KafkaConsumerConfig<K, V> kafkaConsumerConfig) {
        ObjectExtension.requireNonNull(kafkaConsumerConfig, "kafkaConsumerConfig");

        kafkaConsumerConfig = kafkaConsumerConfig.clone();
        setClientId(kafkaConsumerConfig.getProperties());
        Consumer<K, V> consumer = new CConsumerProxy<>(getConfigurationManager(), getMetaManager(),
                kafkaConsumerConfig);
        consumer = CatProxy.newInstance(consumer, Consumer.class);
        _metricsReporter.add(consumer);
        return consumer;
    }

    @SuppressWarnings("resource")
    @Override
    public <K, V> Producer<K, V> newProducer(KafkaProducerConfig<K, V> kafkaProducerConfig) {
        ObjectExtension.requireNonNull(kafkaProducerConfig, "kafkaProducerConfig");

        kafkaProducerConfig = kafkaProducerConfig.clone();
        setClientId(kafkaProducerConfig.getProperties());
        Producer<K, V> producer = new CProducerProxy<>(getConfigurationManager(), getMetaManager(),
                kafkaProducerConfig);
        producer = CatProxy.newInstance(producer, Producer.class);
        _metricsReporter.add(producer);
        return producer;
    }

    protected void setClientId(Properties properties) {
        properties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, getMetaManager().getClientId());
    }

    @Override
    public void close() throws Exception {
        CloseableExtension.close(_metricsReporter);
        super.close();
    }

}
