package com.ctrip.framework.kbear.client;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.mydotey.java.ObjectExtension;
import org.mydotey.java.StringExtension;
import org.mydotey.scf.ConfigurationManager;
import org.mydotey.scf.Property;
import org.mydotey.scf.facade.StringProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.framework.kbear.meta.Cluster;
import com.ctrip.framework.kbear.meta.Topic;
import com.ctrip.framework.kbear.route.Route;

/**
 * @author koqizhao
 *
 * Dec 18, 2018
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class ProducerProxy<K, V> implements Producer<K, V> {

    protected static final String JMX_PREFIX = "kafka.producer";

    private static Logger _logger = LoggerFactory.getLogger(ProducerProxy.class);

    private KafkaMetaManager _metaManager;
    private KafkaProducerConfig _kafkaProducerConfig;
    private String _clientId;

    private Object _addRemoveLock;
    private ConcurrentHashMap<String, ProducerHolder> _producerHolders;

    Property<String, Integer> _destroyDelay;
    private ScheduledExecutorService _executorService;

    public ProducerProxy(ConfigurationManager configurationManager, KafkaMetaManager metaManager,
            KafkaProducerConfig kafkaProducerConfig) {
        ObjectExtension.requireNonNull(configurationManager, "configurationManager");
        ObjectExtension.requireNonNull(metaManager, "metaManager");
        ObjectExtension.requireNonNull(kafkaProducerConfig, "kafkaProducerConfig");

        _clientId = kafkaProducerConfig.getProperties().getProperty(CommonClientConfigs.CLIENT_ID_CONFIG);
        if (StringExtension.isBlank(_clientId))
            _clientId = StringExtension.EMPTY;

        _metaManager = metaManager;
        _kafkaProducerConfig = kafkaProducerConfig;

        _addRemoveLock = new Object();
        _producerHolders = new ConcurrentHashMap<>();

        StringProperties stringProperties = new StringProperties(configurationManager);
        _destroyDelay = stringProperties.getIntProperty("kafka.producer-proxy.producer-destroy-delay", 60 * 1000,
                v -> v < 0 ? null : v);
        _executorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "kafka.producer-proxy.scheduled-executor");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public void initTransactions() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId)
            throws ProducerFencedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        ObjectExtension.requireNonNull(record, "record");
        return getOrAddProducer(record.topic()).getProducer().send(record);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        ObjectExtension.requireNonNull(record, "record");
        ObjectExtension.requireNonNull(callback, "callback");
        return getOrAddProducer(record.topic()).getProducer().send(record, callback);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        ObjectExtension.requireNonNull(topic, "topic");
        return getOrAddProducer(topic).getProducer().partitionsFor(topic);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        Map<MetricName, ? extends Metric> result = new HashMap<>();
        forEach((t, p) -> result.putAll(p.getProducer().metrics()));
        return result;
    }

    @Override
    public void close() {
        synchronized (_addRemoveLock) {
            forEach((t, p) -> removeProducer(t));
            _logger.info("producer closed");
            _executorService.shutdown();
        }
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        synchronized (_addRemoveLock) {
            forEach((t, p) -> removeProducer(t, timeout, unit));
            _logger.info("producer closed");
            _executorService.shutdown();
        }
    }

    protected ProducerHolder getOrAddProducer(String topicId) {
        ObjectExtension.requireNonNull(topicId, "topicId");
        ProducerHolder producerHolder = _producerHolders.get(topicId);
        if (producerHolder != null)
            return producerHolder;

        synchronized (_addRemoveLock) {
            producerHolder = _producerHolders.get(topicId);
            if (producerHolder != null)
                return producerHolder;

            producerHolder = newProducerHolder(topicId);
            _producerHolders.put(topicId, producerHolder);
            _logger.info("producer created: {}", topicId);
            return producerHolder;
        }
    }

    protected ProducerHolder newProducerHolder(String topicId) {
        _metaManager.registerProducer(topicId, () -> restartProducer(topicId));
        KafkaMetaHolder metaHolder = _metaManager.getMetaHolder();
        Route route = metaHolder.getTopicRoutes().get(topicId);
        if (route == null)
            throw new IllegalArgumentException("Topic not found: " + topicId);
        Cluster cluster = metaHolder.getClusters().get(route.getClusterId());
        Topic topic = metaHolder.getTopics().get(route.getTopicId());
        KafkaProducerConfig kafkaProducerConfig = constructProducerConfig(_kafkaProducerConfig, topic, cluster);
        Producer producer = newProducer(topicId, kafkaProducerConfig);
        return new ProducerHolder<>(topicId, kafkaProducerConfig, route, producer);
    }

    protected KafkaProducerConfig constructProducerConfig(KafkaProducerConfig producerConfig, Topic topic,
            Cluster cluster) {
        KafkaProducerConfig kafkaProducerConfig = producerConfig.clone();
        kafkaProducerConfig.getProperties().setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                cluster.getMeta().get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        kafkaProducerConfig.getProperties().setProperty(CommonClientConfigs.CLIENT_ID_CONFIG,
                constructClientId(topic.getId()));
        return kafkaProducerConfig;
    }

    protected String constructClientId(String topicId) {
        return _clientId + "_" + topicId;
    }

    protected Producer newProducer(String topicId, KafkaProducerConfig kafkaProducerConfig) {
        _logger.info("Producer {} created with config: {}", topicId, kafkaProducerConfig);
        return new KafkaProducer<>(kafkaProducerConfig.getProperties(), kafkaProducerConfig.getKeySerializer(),
                kafkaProducerConfig.getValueSerializer());
    }

    protected void removeProducer(String topicId) {
        removeProducer(topicId, c -> c.close());
    }

    protected void removeProducer(String topicId, long timeout, TimeUnit unit) {
        removeProducer(topicId, c -> c.close(timeout, unit));
    }

    protected void removeProducer(String topicId, java.util.function.Consumer<Producer> closer) {
        ProducerHolder producerHolder = _producerHolders.get(topicId);
        if (producerHolder == null)
            return;

        synchronized (_addRemoveLock) {
            producerHolder = _producerHolders.remove(topicId);
            if (producerHolder == null)
                return;

            _metaManager.unregisterProducer(topicId);
            closer.accept(producerHolder.getProducer());
            _logger.info("Producer removed: {}, config: {}", topicId, producerHolder.getConfig());
        }
    }

    protected void restartProducer(String topicId) {
        synchronized (_addRemoveLock) {
            ProducerHolder oldProducerHolder = _producerHolders.remove(topicId);
            _metaManager.unregisterProducer(topicId);
            KafkaAppInfoExtension.unregister(JMX_PREFIX, constructClientId(topicId));

            ProducerHolder newProducerHolder = newProducerHolder(topicId);
            _producerHolders.put(topicId, newProducerHolder);

            _executorService.schedule(() -> {
                try {
                    oldProducerHolder.getProducer().close();
                } catch (Throwable e) {
                    _logger.error("close old producer failed for topic: " + topicId, e);
                }
            }, _destroyDelay.getValue(), TimeUnit.MILLISECONDS);
        }
    }

    protected void forEach(BiConsumer<String, ProducerHolder> biConsumer) {
        forEach(t -> true, biConsumer);
    }

    protected void forEach(Predicate<String> predicate, BiConsumer<String, ProducerHolder> biConsumer) {
        Collection<String> topics = _producerHolders.keySet().stream().filter(predicate).collect(Collectors.toSet());
        topics.forEach(t -> biConsumer.accept(t, _producerHolders.get(t)));
    }

}
