package com.ctrip.framework.kbear.client;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.mydotey.java.ObjectExtension;
import org.mydotey.java.StringExtension;
import org.mydotey.scf.ConfigurationManager;
import org.mydotey.scf.Property;
import org.mydotey.scf.facade.StringProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.framework.kbear.meta.Cluster;
import com.ctrip.framework.kbear.meta.ConsumerGroup;
import com.ctrip.framework.kbear.meta.ConsumerGroupId;
import com.ctrip.framework.kbear.meta.Topic;
import com.ctrip.framework.kbear.route.Route;

/**
 * @author koqizhao
 *
 * Dec 18, 2018
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class ConsumerProxy<K, V> implements Consumer<K, V> {

    private static Logger _logger = LoggerFactory.getLogger(ConsumerProxy.class);

    protected static final long NO_CURRENT_THREAD = -1L;
    protected static final String JMX_PREFIX = "kafka.consumer";

    private KafkaMetaManager _metaManager;
    private KafkaConsumerConfig _kafkaConsumerConfig;
    private String _clientId;
    private String _groupName;

    private ConcurrentHashMap<String, ConsumerHolder> _consumerHolders;
    private ExecutorService _executorService;

    Property<String, Integer> _destroyDelay;
    private ScheduledExecutorService _scheduledExecutorService;

    private Field _recordsField;

    private volatile boolean _closed;
    private AtomicLong _currentThread;
    private AtomicInteger _refcount;

    private Object _addRemoveLock;

    public ConsumerProxy(ConfigurationManager configurationManager, KafkaMetaManager metaManager,
            KafkaConsumerConfig kafkaConsumerConfig) {
        ObjectExtension.requireNonNull(configurationManager, "configurationManager");
        ObjectExtension.requireNonNull(metaManager, "metaManager");
        ObjectExtension.requireNonNull(kafkaConsumerConfig, "kafkaConsumerConfig");

        _clientId = kafkaConsumerConfig.getProperties().getProperty(CommonClientConfigs.CLIENT_ID_CONFIG);
        if (StringExtension.isBlank(_clientId))
            _clientId = StringExtension.EMPTY;
        _groupName = kafkaConsumerConfig.getProperties().getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        ObjectExtension.requireNonBlank(_groupName, ConsumerConfig.GROUP_ID_CONFIG);

        _metaManager = metaManager;
        _kafkaConsumerConfig = kafkaConsumerConfig;
        _consumerHolders = new ConcurrentHashMap<>();

        AtomicInteger threadCount = new AtomicInteger();
        _executorService = Executors.newCachedThreadPool(r -> {
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            thread.setName("kafka.consumer-proxy.executor-" + _clientId + "-" + threadCount.getAndIncrement());
            return thread;
        });

        StringProperties stringProperties = new StringProperties(configurationManager);
        _destroyDelay = stringProperties.getIntProperty("kafka.consumer-proxy.consumer-destroy-delay", 1 * 1000,
                v -> v < 0 ? null : v);
        _scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "kafka.consumer-proxy.scheduled-executor");
            thread.setDaemon(true);
            return thread;
        });

        try {
            _recordsField = ConsumerRecords.class.getDeclaredField("records");
            _recordsField.setAccessible(true);
        } catch (NoSuchFieldException | SecurityException e) {
            throw new UnsupportedOperationException("ConsumerRecords has not a field 'records'", e);
        }

        _currentThread = new AtomicLong(NO_CURRENT_THREAD);
        _refcount = new AtomicInteger();

        _addRemoveLock = new Object();
    }

    @Override
    public Set<TopicPartition> assignment() {
        return runWithoutConcurrency(() -> {
            HashSet<TopicPartition> result = new HashSet<>();
            _consumerHolders.values().forEach(c -> result.addAll(c.getConsumer().assignment()));
            return Collections.unmodifiableSet(result);
        });
    }

    @Override
    public Set<String> subscription() {
        return runWithoutConcurrency(() -> Collections.unmodifiableSet(new HashSet<>(_consumerHolders.keySet())));
    }

    @Override
    public void subscribe(Collection<String> topics) {
        ObjectExtension.requireNonNull(topics, "topics");

        runWithoutConcurrency(() -> {
            synchronized (_addRemoveLock) {
                removeNotUsed(topics);
                topics.forEach(t -> {
                    ConsumerHolder consumerHolder = getOrAddConsumer(t);
                    consumerHolder.getConsumer().subscribe(Arrays.asList(t));
                    consumerHolder.setConsumerRebalanceListener(null);
                    consumerHolder.setAssignments(null);
                });
            }
        });
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        ObjectExtension.requireNonNull(topics, "topics");
        ObjectExtension.requireNonNull(callback, "callback");

        runWithoutConcurrency(() -> {
            synchronized (_addRemoveLock) {
                removeNotUsed(topics);
                topics.forEach(t -> {
                    ConsumerHolder consumerHolder = getOrAddConsumer(t);
                    consumerHolder.getConsumer().subscribe(Arrays.asList(t), callback);
                    consumerHolder.setConsumerRebalanceListener(callback);
                    consumerHolder.setAssignments(null);
                });
            }
        });
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        ObjectExtension.requireNonNull(partitions, "partitions");

        runWithoutConcurrency(() -> {
            synchronized (_addRemoveLock) {
                if (partitions.isEmpty()) {
                    unsubscribe();
                    return;
                }

                Map<String, Collection<TopicPartition>> byTopic = toMap(partitions);
                removeNotUsed(byTopic.keySet());
                byTopic.entrySet().forEach(e -> {
                    ConsumerHolder consumerHolder = getOrAddConsumer(e.getKey());
                    consumerHolder.getConsumer().assign(e.getValue());
                    consumerHolder.setAssignments(e.getValue());
                    consumerHolder.setConsumerRebalanceListener(null);
                });
            }
        });
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void subscribe(Pattern pattern) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unsubscribe() {
        runWithoutConcurrency(() -> {
            synchronized (_addRemoveLock) {
                _consumerHolders.keySet().forEach(this::removeConsumer);
            }
        });
    }

    @SuppressWarnings("deprecation")
    @Override
    public ConsumerRecords<K, V> poll(long timeout) {
        if (timeout < 0)
            throw new IllegalArgumentException("timeout must not be negative");

        return runWithoutConcurrency(() -> {
            Map<TopicPartition, List<ConsumerRecord>> result = new HashMap();
            forEach((t, c) -> result.putAll(toMap(c.getConsumer().poll(timeout))));
            return new ConsumerRecords(result);
        });
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        ObjectExtension.requireNonNull(timeout, "timeout");
        if (timeout.toMillis() < 0)
            throw new IllegalArgumentException("timeout must not be negative");

        return runWithoutConcurrency(() -> {
            Map<TopicPartition, List<ConsumerRecord>> result = new HashMap();
            forEach((t, c) -> result.putAll(toMap(c.getConsumer().poll(timeout))));
            return new ConsumerRecords(result);
        });
    }

    @Override
    public void commitSync() {
        runWithoutConcurrency(() -> forEach((t, c) -> c.getConsumer().commitSync()));
    }

    @Override
    public void commitSync(Duration timeout) {
        ObjectExtension.requireNonNull(timeout, "timeout");
        if (timeout.toMillis() < 0)
            throw new IllegalArgumentException("timeout must not be negative");

        runWithoutConcurrency(() -> forEach((t, c) -> c.getConsumer().commitSync(timeout)));
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        Map<String, Map<TopicPartition, OffsetAndMetadata>> map = toMap(offsets);
        runWithoutConcurrency(() -> forEach(map::containsKey, (t, c) -> c.getConsumer().commitSync(map.get(t))));
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        ObjectExtension.requireNonNull(timeout, "timeout");
        if (timeout.toMillis() < 0)
            throw new IllegalArgumentException("timeout must not be negative");

        Map<String, Map<TopicPartition, OffsetAndMetadata>> map = toMap(offsets);
        runWithoutConcurrency(
                () -> forEach(map::containsKey, (t, c) -> c.getConsumer().commitSync(map.get(t), timeout)));
    }

    @Override
    public void commitAsync() {
        runWithoutConcurrency(() -> _consumerHolders.values().forEach(c -> c.getConsumer().commitAsync()));
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        runWithoutConcurrency(() -> _consumerHolders.values().forEach(c -> c.getConsumer().commitAsync(callback)));
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        ObjectExtension.requireNonNull(offsets, "offsets");

        runWithoutConcurrency(() -> {
            Map<String, Map<TopicPartition, OffsetAndMetadata>> byTopic = new HashMap<>();
            offsets.forEach((tp, oam) -> byTopic.computeIfAbsent(tp.topic(), k -> new HashMap<>()).put(tp, oam));
            byTopic.forEach((t, os) -> _consumerHolders.get(t).getConsumer().commitAsync(os, callback));
        });
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        ObjectExtension.requireNonNull(partition, "partition");
        if (offset < 0)
            throw new IllegalArgumentException("seek offset must not be a negative number");

        runWithoutConcurrency(() -> getOrAddConsumer(partition.topic()).getConsumer().seek(partition, offset));
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        Map<String, Collection<TopicPartition>> byTopic = toMap(partitions);
        runWithoutConcurrency(() -> byTopic.forEach((t, ps) -> getOrAddConsumer(t).getConsumer().seekToBeginning(ps)));
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        Map<String, Collection<TopicPartition>> byTopic = toMap(partitions);
        runWithoutConcurrency(() -> byTopic.forEach((t, ps) -> getOrAddConsumer(t).getConsumer().seekToEnd(ps)));
    }

    @Override
    public long position(TopicPartition partition) {
        ObjectExtension.requireNonNull(partition, "partition");
        return runWithoutConcurrency(() -> getOrAddConsumer(partition.topic()).getConsumer().position(partition));
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        ObjectExtension.requireNonNull(partition, "partition");
        return runWithoutConcurrency(
                () -> getOrAddConsumer(partition.topic()).getConsumer().position(partition, timeout));
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        ObjectExtension.requireNonNull(partition, "partition");
        return runWithoutConcurrency(() -> getOrAddConsumer(partition.topic()).getConsumer().committed(partition));
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        ObjectExtension.requireNonNull(partition, "partition");
        return runWithoutConcurrency(
                () -> getOrAddConsumer(partition.topic()).getConsumer().committed(partition, timeout));
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        Map<MetricName, ? extends Metric> result = new HashMap<>();
        _consumerHolders.forEach((t, c) -> result.putAll(c.getConsumer().metrics()));
        return Collections.unmodifiableMap(result);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        ObjectExtension.requireNonNull(topic, "topic");
        return runWithoutConcurrency(() -> getOrAddConsumer(topic).getConsumer().partitionsFor(topic));
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        ObjectExtension.requireNonNull(topic, "topic");
        ObjectExtension.requireNonNull(timeout, "timeout");
        if (timeout.toMillis() < 0)
            throw new IllegalArgumentException("timeout must not be negative");

        return runWithoutConcurrency(() -> getOrAddConsumer(topic).getConsumer().partitionsFor(topic, timeout));
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return runWithoutConcurrency(() -> {
            Map<String, List<PartitionInfo>> results = new HashMap<>();
            forEach((t, c) -> results.putAll(c.getConsumer().listTopics()));
            return Collections.unmodifiableMap(results);
        });
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.toMillis() < 0)
            throw new IllegalArgumentException("timeout must not be negative");

        return runWithoutConcurrency(() -> {
            Map<String, List<PartitionInfo>> results = new HashMap<>();
            forEach((t, c) -> results.putAll(c.getConsumer().listTopics(timeout)));
            return Collections.unmodifiableMap(results);
        });
    }

    @Override
    public Set<TopicPartition> paused() {
        return runWithoutConcurrency(() -> {
            Set<TopicPartition> results = new HashSet<>();
            forEach((t, c) -> results.addAll(c.getConsumer().paused()));
            return Collections.unmodifiableSet(results);
        });
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        Map<String, Collection<TopicPartition>> byTopic = toMap(partitions);
        runWithoutConcurrency(() -> byTopic.forEach((t, ps) -> _consumerHolders.get(t).getConsumer().pause(ps)));
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        Map<String, Collection<TopicPartition>> byTopic = toMap(partitions);
        runWithoutConcurrency(() -> byTopic.forEach((t, ps) -> _consumerHolders.get(t).getConsumer().resume(ps)));
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        ObjectExtension.requireNonNull(timestampsToSearch, "timestampsToSearch");

        return runWithoutConcurrency(() -> {
            Map<String, Map<TopicPartition, Long>> byTopic = new HashMap<>();
            timestampsToSearch
                    .forEach((tp, ts) -> byTopic.computeIfAbsent(tp.topic(), k -> new HashMap<>()).put(tp, ts));
            Map<TopicPartition, OffsetAndTimestamp> result = new HashMap<>();
            forEach(byTopic::containsKey, (t, c) -> result.putAll(c.getConsumer().offsetsForTimes(byTopic.get(t))));
            return Collections.unmodifiableMap(result);
        });
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch,
            Duration timeout) {
        ObjectExtension.requireNonNull(timestampsToSearch, "timestampsToSearch");
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.toMillis() < 0)
            throw new IllegalArgumentException("timeout must not be negative");

        return runWithoutConcurrency(() -> {
            Map<String, Map<TopicPartition, Long>> byTopic = new HashMap<>();
            timestampsToSearch
                    .forEach((tp, ts) -> byTopic.computeIfAbsent(tp.topic(), k -> new HashMap<>()).put(tp, ts));
            Map<TopicPartition, OffsetAndTimestamp> result = new HashMap<>();
            forEach(byTopic::containsKey,
                    (t, c) -> result.putAll(c.getConsumer().offsetsForTimes(byTopic.get(t), timeout)));
            return Collections.unmodifiableMap(result);
        });
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        Map<String, Collection<TopicPartition>> map = toMap(partitions);
        return runWithoutConcurrency(() -> {
            Map<TopicPartition, Long> result = new HashMap<>();
            forEach(map::containsKey, (t, c) -> result.putAll(c.getConsumer().beginningOffsets(map.get(t))));
            return Collections.unmodifiableMap(result);
        });
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.toMillis() < 0)
            throw new IllegalArgumentException("timeout must not be negative");

        Map<String, Collection<TopicPartition>> map = toMap(partitions);
        return runWithoutConcurrency(() -> {
            Map<TopicPartition, Long> result = new HashMap<>();
            forEach(map::containsKey, (t, c) -> result.putAll(c.getConsumer().beginningOffsets(map.get(t), timeout)));
            return Collections.unmodifiableMap(result);
        });
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        Map<String, Collection<TopicPartition>> map = toMap(partitions);
        return runWithoutConcurrency(() -> {
            Map<TopicPartition, Long> result = new HashMap<>();
            forEach(map::containsKey, (t, c) -> result.putAll(c.getConsumer().endOffsets(map.get(t))));
            return Collections.unmodifiableMap(result);
        });
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.toMillis() < 0)
            throw new IllegalArgumentException("timeout must not be negative");

        Map<String, Collection<TopicPartition>> map = toMap(partitions);
        return runWithoutConcurrency(() -> {
            Map<TopicPartition, Long> result = new HashMap<>();
            forEach(map::containsKey, (t, c) -> result.putAll(c.getConsumer().endOffsets(map.get(t), timeout)));
            return Collections.unmodifiableMap(result);
        });
    }

    @Override
    public void close() {
        acquire();
        try {
            if (!_closed) {
                synchronized (_addRemoveLock) {
                    if (!_closed) {
                        _consumerHolders.keySet().forEach(this::removeConsumer);
                        doClose();
                    }
                }
            }
        } finally {
            release();
        }
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        if (timeout < 0)
            throw new IllegalArgumentException("timeout cannot be negative");
        ObjectExtension.requireNonNull(unit, "unit");

        acquire();
        try {
            if (!_closed) {
                synchronized (_addRemoveLock) {
                    if (!_closed) {
                        _consumerHolders.keySet().forEach(t -> removeConsumer(t, timeout, unit));
                        doClose();
                    }
                }
            }
        } finally {
            release();
        }
    }

    @Override
    public void close(Duration timeout) {
        ObjectExtension.requireNonNull("timeout", "timeout");
        if (timeout.toMillis() < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");

        acquire();
        try {
            if (!_closed) {
                synchronized (_addRemoveLock) {
                    if (!_closed) {
                        _consumerHolders.keySet().forEach(t -> removeConsumer(t, timeout));
                        doClose();
                    }
                }
            }
        } finally {
            release();
        }
    }

    protected void doClose() {
        _closed = true;
        _logger.info("consumer closed");
        _executorService.shutdown();
        _scheduledExecutorService.shutdown();
    }

    @Override
    public void wakeup() {
        forEach((t, c) -> c.getConsumer().wakeup());
    }

    protected ConsumerHolder getOrAddConsumer(String topicId) {
        ConsumerHolder consumerHolder = _consumerHolders.get(topicId);
        if (consumerHolder == null) {
            synchronized (_addRemoveLock) {
                consumerHolder = _consumerHolders.get(topicId);
                if (consumerHolder == null) {
                    consumerHolder = newConsumerHolder(topicId);
                    _consumerHolders.put(topicId, consumerHolder);
                    _logger.info("consumer created: {}", new ConsumerGroupId(_groupName, topicId));
                }
            }
        }

        return consumerHolder;
    }

    protected void removeNotUsed(Collection<String> topics) {
        synchronized (_addRemoveLock) {
            Set<String> existing = new HashSet<>(_consumerHolders.keySet());
            existing.removeAll(topics);
            existing.forEach(this::removeConsumer);
        }
    }

    protected void removeConsumer(String topicId) {
        removeConsumer(topicId, c -> {
            c.close();
        });
    }

    @SuppressWarnings("deprecation")
    protected void removeConsumer(String topicId, long timeout, TimeUnit unit) {
        removeConsumer(topicId, c -> {
            c.close(timeout, unit);
        });
    }

    protected void removeConsumer(String topicId, Duration timeout) {
        removeConsumer(topicId, c -> {
            c.close(timeout);
        });
    }

    protected void removeConsumer(String topicId, java.util.function.Consumer<Consumer> closer) {
        synchronized (_addRemoveLock) {
            ConsumerHolder consumerHolder = _consumerHolders.remove(topicId);
            if (consumerHolder == null)
                return;

            ConsumerGroupId consumerGroupId = new ConsumerGroupId(_groupName, topicId);
            _metaManager.unregisterConsumer(consumerGroupId);
            closer.accept(consumerHolder.getConsumer());
            _logger.info("Consumer removed: {}, config: {}", consumerGroupId, consumerHolder.getConfig());
        }
    }

    protected ConsumerHolder newConsumerHolder(String topicId) {
        ConsumerGroupId consumerGroupId = new ConsumerGroupId(_groupName, topicId);
        _metaManager.registerConsumer(consumerGroupId, () -> _executorService.submit(() -> restartConsumer(topicId)));
        KafkaMetaHolder metaHolder = _metaManager.getMetaHolder();
        Route route = metaHolder.getConsumerGroupRoutes().get(consumerGroupId);
        if (route == null)
            throw new IllegalArgumentException("ConsumerGroup not found: " + consumerGroupId);
        ConsumerGroup consumerGroup = metaHolder.getConsumerGroups().get(consumerGroupId);
        Cluster cluster = metaHolder.getClusters().get(route.getClusterId());
        Topic topic = metaHolder.getTopics().get(route.getTopicId());
        KafkaConsumerConfig kafkaConsumerConfig = constructConsumerConfig(_kafkaConsumerConfig, consumerGroup, topic,
                cluster);
        Consumer consumer = newConsumer(consumerGroupId, kafkaConsumerConfig);
        return new ConsumerHolder<>(consumerGroupId, kafkaConsumerConfig, route, consumer);
    }

    protected KafkaConsumerConfig constructConsumerConfig(KafkaConsumerConfig consumerConfig,
            ConsumerGroup consumerGroup, Topic topic, Cluster cluster) {
        KafkaConsumerConfig kafkaConsumerConfig = consumerConfig.clone();
        kafkaConsumerConfig.getProperties().setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                cluster.getMeta().get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        kafkaConsumerConfig.getProperties().setProperty(CommonClientConfigs.CLIENT_ID_CONFIG,
                constructClientId(consumerGroup.getId()));
        return kafkaConsumerConfig;
    }

    protected String constructClientId(ConsumerGroupId consumerGroupId) {
        return _clientId + "_" + consumerGroupId.getTopicId() + "_" + consumerGroupId.getGroupName();
    }

    protected Consumer newConsumer(ConsumerGroupId consumerGroupId, KafkaConsumerConfig kafkaConsumerConfig) {
        _logger.info("Consumer {} created with config: {}", consumerGroupId, kafkaConsumerConfig);
        return new KafkaConsumer<>(kafkaConsumerConfig.getProperties(), kafkaConsumerConfig.getKeyDeserializer(),
                kafkaConsumerConfig.getValueDeserializer());
    }

    protected void restartConsumer(String topicId) {
        synchronized (_addRemoveLock) {
            ConsumerGroupId consumerGroupId = new ConsumerGroupId(_groupName, topicId);
            _metaManager.unregisterConsumer(consumerGroupId);

            ConsumerHolder oldConsumerHolder = _consumerHolders.remove(topicId);
            if (oldConsumerHolder == null)
                return;

            ConsumerRestartListener consumerRestartListener = oldConsumerHolder.getConfig()
                    .getConsumerRestartListener();
            if (consumerRestartListener != null) {
                try {
                    consumerRestartListener.beforeRestart(consumerGroupId.clone(), oldConsumerHolder.getConsumer());
                } catch (Exception e) {
                    _logger.error("consumerRestartListener.beforeRestart failed for consumer: " + consumerGroupId, e);
                }
            }

            closeConsumer(oldConsumerHolder);

            ConsumerHolder newConsumerHolder = getOrAddConsumer(topicId);
            if (oldConsumerHolder.getAssignments() != null) {
                newConsumerHolder.setAssignments(oldConsumerHolder.getAssignments());
                oldConsumerHolder.getConsumer().assign(oldConsumerHolder.getAssignments());
            } else if (oldConsumerHolder.getConsumerRebalanceListener() != null) {
                newConsumerHolder.setConsumerRebalanceListener(oldConsumerHolder.getConsumerRebalanceListener());
                newConsumerHolder.getConsumer().subscribe(Arrays.asList(topicId),
                        oldConsumerHolder.getConsumerRebalanceListener());
            } else {
                newConsumerHolder.getConsumer().subscribe(Arrays.asList(topicId));
            }

            if (consumerRestartListener != null) {
                try {
                    consumerRestartListener.afterRestart(consumerGroupId, newConsumerHolder.getConsumer());
                } catch (Exception e) {
                    _logger.error("consumerRestartListener.afterRestart failed for consumer: " + consumerGroupId, e);
                }
            }
        }
    }

    protected void closeConsumer(ConsumerHolder consumerHolder) {
        try {
            consumerHolder.getConsumer().close();
            _logger.info("Old consumer removed: {}, config: {}", consumerHolder.getConsumerGroupId(),
                    consumerHolder.getConfig());
        } catch (ConcurrentModificationException e) {
            _logger.info("Old consumer {} is used currently, delay {} ms to destroy",
                    consumerHolder.getConsumerGroupId(), _destroyDelay.getValue());
            _scheduledExecutorService.schedule(() -> closeConsumer(consumerHolder), _destroyDelay.getValue(),
                    TimeUnit.MILLISECONDS);
        } catch (Throwable e) {
            _logger.warn("Close old consumer " + consumerHolder.getConsumerGroupId() + " failed", e);
        }
    }

    protected Map<String, Collection<TopicPartition>> toMap(Collection<TopicPartition> partitions) {
        ObjectExtension.requireNonNull(partitions, "partitions");

        Map<String, Collection<TopicPartition>> byTopic = new HashMap<>();
        partitions.forEach(p -> byTopic.computeIfAbsent(p.topic(), k -> new ArrayList<>()).add(p));
        return byTopic;
    }

    protected Map<TopicPartition, List<ConsumerRecord>> toMap(ConsumerRecords records) {
        ObjectExtension.requireNonNull(records, "records");

        try {
            return (Map<TopicPartition, List<ConsumerRecord>>) _recordsField.get(records);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new UnsupportedOperationException(e);
        }
    }

    protected Map<String, Map<TopicPartition, OffsetAndMetadata>> toMap(
            Map<TopicPartition, OffsetAndMetadata> offsets) {
        ObjectExtension.requireNonNull(offsets, "offsets");
        Map<String, Map<TopicPartition, OffsetAndMetadata>> result = new HashMap<>();
        offsets.forEach((tp, os) -> result.computeIfAbsent(tp.topic(), k -> new HashMap<>()).put(tp, os));
        return result;
    }

    protected void forEach(BiConsumer<String, ConsumerHolder> biConsumer) {
        forEach(t -> true, biConsumer);
    }

    protected void forEach(Predicate<String> predicate, BiConsumer<String, ConsumerHolder> biConsumer) {
        Collection<String> topics = _consumerHolders.keySet().stream().filter(predicate).collect(Collectors.toSet());
        CountDownLatch latch = new CountDownLatch(topics.size());
        topics.forEach(t -> {
            _executorService.execute(() -> {
                try {
                    biConsumer.accept(t, _consumerHolders.get(t));
                } catch (WakeupException e) {
                    _logger.info("foreach was waken up");
                } catch (Throwable e) {
                    _logger.error("foreach failed for consumer", e);
                } finally {
                    latch.countDown();
                }
            });
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    protected void acquireAndEnsureOpen() {
        acquire();
        if (this._closed) {
            release();
            throw new IllegalStateException("This consumer has already been closed.");
        }
    }

    protected void acquire() {
        long threadId = Thread.currentThread().getId();
        if (threadId != _currentThread.get() && !_currentThread.compareAndSet(NO_CURRENT_THREAD, threadId))
            throw new ConcurrentModificationException("KafkaConsumer is not safe for multi-threaded access");
        _refcount.incrementAndGet();
    }

    protected void release() {
        if (_refcount.decrementAndGet() == 0)
            _currentThread.set(NO_CURRENT_THREAD);
    }

    protected void runWithoutConcurrency(Runnable action) {
        acquireAndEnsureOpen();
        try {
            action.run();
        } finally {
            release();
        }
    }

    protected <T> T runWithoutConcurrency(Supplier<T> supplier) {
        acquireAndEnsureOpen();
        try {
            return supplier.get();
        } finally {
            release();
        }
    }

}
