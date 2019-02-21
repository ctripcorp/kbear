package com.ctrip.framework.kbear.client;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.junit.Assert;
import org.junit.Test;
import org.mydotey.java.collection.CollectionExtension;

/**
 * @author koqizhao
 *
 * Jan 23, 2019
 */
public class ConsumerTest extends KbearTestBase {

    private Set<String> _topics = new HashSet<>(Arrays.asList(TestData.TOPIC_9, TestData.TOPIC_10, TestData.TOPIC_11));

    private TopicPartition _topicPartition = new TopicPartition(TestData.TOPIC_9, 0);

    private Set<TopicPartition> _topicPartitions = new HashSet<>(Arrays.asList(new TopicPartition(TestData.TOPIC_9, 0),
            new TopicPartition(TestData.TOPIC_10, 0), new TopicPartition(TestData.TOPIC_11, 0)));

    private int _messageCount = 10;

    private String _consumerGroup = TestData.CONSUMER_GROUP;

    private long _waitTimeout = 10 * 60 * 1000;

    private long _sendInterval = 100;

    private long _pollTimeout = 180;

    @Test
    public void assignment() throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumer()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);

            Set<TopicPartition> assignments = consumer.assignment();
            System.out.println("assignments: " + assignments);
            Assert.assertEquals(_topicPartitions, assignments);
        }
    }

    @Test
    public void subscription() throws InterruptedException {
        try (Consumer<String, String> consumer = createConsumer()) {
            consumer.subscribe(_topics);

            Set<String> subscriptions = consumer.subscription();
            System.out.println("subscriptions: " + subscriptions);
            Assert.assertEquals(subscriptions, _topics);
        }
    }

    @Test
    public void subscribe() throws InterruptedException {
        pollDurationTimeout();
    }

    @Test
    public void subscribeWithListener() throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumer()) {
            AtomicBoolean revokeCalled = new AtomicBoolean();
            AtomicBoolean assignCalled = new AtomicBoolean();
            consumer.subscribe(_topics, new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    revokeCalled.set(true);
                    System.out.println("onPartitionsRevoked: " + partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    assignCalled.set(true);
                    System.out.println("onPartitionsAssigned: " + partitions);
                }
            });

            pollDurationTimeout(consumer);

            Assert.assertTrue(revokeCalled.get());
            Assert.assertTrue(assignCalled.get());
        }
    }

    @Test
    public void assign() throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumer()) {
            consumer.assign(_topicPartitions);
            pollDurationTimeout(consumer);

            Set<TopicPartition> assignments = consumer.assignment();
            System.out.println("assignments: " + assignments);
            Assert.assertEquals(_topicPartitions, assignments);
        }
    }

    @Test
    public void unsubscribe() throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumer()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);

            Set<TopicPartition> assignments = consumer.assignment();
            System.out.println("assignments: " + assignments);
            Assert.assertEquals(_topicPartitions, assignments);

            consumer.unsubscribe();

            assignments = consumer.assignment();
            System.out.println("assignments: " + assignments);
            Assert.assertTrue(CollectionExtension.isEmpty(assignments));
        }
    }

    @Test
    public void pollLongTimeout() throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumer()) {
            consumer.subscribe(_topics);
            pollLongTimeout(consumer);
        }
    }

    @Test
    public void pollDurationTimeout() throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumer()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);
        }
    }

    @Test
    public void commitSync() throws InterruptedException {
        commitSync(c -> c.commitSync());
    }

    @Test
    public void commitSyncWithTimeout() throws InterruptedException {
        Duration timeout = Duration.ofMillis(_waitTimeout);
        commitSync(c -> c.commitSync(timeout));
    }

    protected void commitSync(java.util.function.Consumer<Consumer<String, String>> committer)
            throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumerWithoutAutoCommit()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);

            OffsetAndMetadata committed = consumer.committed(_topicPartition);
            System.out.println("committed: " + committed);
            committer.accept(consumer);
            OffsetAndMetadata committed2 = consumer.committed(_topicPartition);
            System.out.println("committed2: " + committed2);
            Assert.assertTrue(committed2.offset() > committed.offset());
        }
    }

    @Test
    public void commitSyncWithOffsetMap() throws InterruptedException {
        commitSync((c, m) -> c.commitSync(m));
    }

    @Test
    public void commitSyncWithOffsetMapAndTimeout() throws InterruptedException {
        Duration timeout = Duration.ofMillis(_waitTimeout);
        commitSync((c, m) -> c.commitSync(m, timeout));
    }

    protected void commitSync(
            java.util.function.BiConsumer<Consumer<String, String>, Map<TopicPartition, OffsetAndMetadata>> committer)
            throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumerWithoutAutoCommit()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);

            OffsetAndMetadata committed = consumer.committed(_topicPartition);
            System.out.println("committed: " + committed);
            OffsetAndMetadata committed2 = new OffsetAndMetadata(committed.offset() + _messageCount,
                    committed.metadata());
            System.out.println("committed2: " + committed2);
            Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
            offsetMap.put(_topicPartition, committed2);
            committer.accept(consumer, offsetMap);
            OffsetAndMetadata committed3 = consumer.committed(_topicPartition);
            System.out.println("committed3: " + committed3);
            Assert.assertEquals(committed2.offset(), committed3.offset());
        }
    }

    @Test
    public void commitAsync() throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumerWithoutAutoCommit()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);

            OffsetAndMetadata committed = consumer.committed(_topicPartition);
            System.out.println("committed: " + committed);
            consumer.commitAsync();
            Thread.sleep(30 * 1000);
            OffsetAndMetadata committed2 = consumer.committed(_topicPartition);
            System.out.println("committed2: " + committed2);
            Assert.assertTrue(committed2.offset() > committed.offset());
        }
    }

    @Test
    public void commitAsyncWithCallback() throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumerWithoutAutoCommit()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);

            OffsetAndMetadata committed = consumer.committed(_topicPartition);
            System.out.println("committed: " + committed);
            CountDownLatch latch = new CountDownLatch(_topics.size());
            AtomicBoolean failed = new AtomicBoolean();
            consumer.commitAsync((m, e) -> {
                if (e != null) {
                    failed.set(true);
                    e.printStackTrace();
                } else
                    System.out.println("offsetsMap: " + m);

                latch.countDown();
            });

            long timeout = 30 * 1000;
            Thread.sleep(timeout);
            _messageCount = 0;
            _pollTimeout = 100;
            pollDurationTimeout(consumer); // callback is invoked when next poll or commit

            if (!latch.await(1000, TimeUnit.MILLISECONDS))
                Assert.fail("commitAsyncWithCallback wait timeout: " + timeout);

            Assert.assertFalse(failed.get());
            OffsetAndMetadata committed2 = consumer.committed(_topicPartition);
            System.out.println("committed2: " + committed2);
            Assert.assertTrue(committed2.offset() > committed.offset());
        }
    }

    @Test
    public void commitAsyncWithOffsetsAndCallback() throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumerWithoutAutoCommit()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);

            OffsetAndMetadata committed = consumer.committed(_topicPartition);
            System.out.println("committed: " + committed);
            OffsetAndMetadata committed2 = new OffsetAndMetadata(committed.offset() + _messageCount,
                    committed.metadata());
            System.out.println("committed2: " + committed2);
            Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
            offsetMap.put(_topicPartition, committed2);
            CountDownLatch latch = new CountDownLatch(offsetMap.size());
            AtomicBoolean failed = new AtomicBoolean();
            consumer.commitAsync(offsetMap, (m, e) -> {
                if (e != null) {
                    failed.set(true);
                    e.printStackTrace();
                } else
                    System.out.println("offsetsMap: " + m);

                latch.countDown();
            });

            long timeout = 30 * 1000;
            Thread.sleep(timeout);
            _messageCount = 0;
            _pollTimeout = 100;
            pollDurationTimeout(consumer); // callback is invoked when next poll or commit

            if (!latch.await(1000, TimeUnit.MILLISECONDS))
                Assert.fail("commitAsyncWithCallback wait timeout: " + timeout);

            Assert.assertFalse(failed.get());
            OffsetAndMetadata committed3 = consumer.committed(_topicPartition);
            System.out.println("committed3: " + committed3);
            Assert.assertEquals(committed2.offset(), committed3.offset());
        }
    }

    @Test
    public void seek() throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumer()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);

            System.out.println("\nseek\n");
            _topicPartitions.forEach(tp -> {
                OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
                consumer.seek(tp, offsetAndMetadata.offset() - _messageCount);
            });

            System.out.println("\nre-poll\n");
            pollDurationTimeout(consumer);
        }
    }

    @Test
    public void seekToBeginning() throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumer()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);

            System.out.println("\nseek\n");
            consumer.seekToBeginning(_topicPartitions);

            System.out.println("\nre-poll\n");
            pollDurationTimeout(consumer);
        }
    }

    @Test
    public void seekToEnd() throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumer()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);

            produceMessages();

            System.out.println("\nseek\n");
            consumer.seekToEnd(_topicPartitions);

            System.out.println("\nsecond-poll\n");
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(30 * 1000));
            Assert.assertEquals(0, consumerRecords.count());
        }
    }

    @Test
    public void position() throws InterruptedException {
        position((c, tp) -> c.position(tp));
    }

    @Test
    public void positionWithTimeout() throws InterruptedException {
        Duration timeout = Duration.ofMillis(_waitTimeout);
        position((c, tp) -> c.position(tp, timeout));
    }

    protected void position(BiFunction<Consumer<String, String>, TopicPartition, Long> positionFetcher)
            throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumerWithoutAutoCommit()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);

            consumer.commitSync();

            OffsetAndMetadata offsetAndMetadata = consumer.committed(_topicPartition);
            System.out.println("offsetAndMetadata: " + offsetAndMetadata);
            long position = positionFetcher.apply(consumer, _topicPartition);
            System.out.println("position: " + position);
            Assert.assertEquals(offsetAndMetadata.offset(), position);
        }
    }

    @Test
    public void committed() throws InterruptedException {
        committed((c, tp) -> c.committed(tp));
    }

    @Test
    public void committedWithTimeout() throws InterruptedException {
        Duration timeout = Duration.ofMillis(_waitTimeout);
        committed((c, tp) -> c.committed(tp, timeout));
    }

    protected void committed(BiFunction<Consumer<String, String>, TopicPartition, OffsetAndMetadata> committedFetcher)
            throws InterruptedException {
        produceMessages();

        OffsetAndMetadata offsetAndMetadata;
        try (Consumer<String, String> consumer = createConsumer()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);
            offsetAndMetadata = committedFetcher.apply(consumer, _topicPartition);
            System.out.println("offset: " + offsetAndMetadata);
            Assert.assertNotNull(offsetAndMetadata);
        }

        produceMessages();

        OffsetAndMetadata offsetAndMetadata2;
        try (Consumer<String, String> consumer = createConsumer()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);
            offsetAndMetadata2 = committedFetcher.apply(consumer, _topicPartition);
            System.out.println("offset2: " + offsetAndMetadata2);
            Assert.assertNotNull(offsetAndMetadata2);
        }

        Assert.assertTrue(offsetAndMetadata2.offset() > offsetAndMetadata.offset());
    }

    @Test
    public void metrics() throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumer()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);

            Map<MetricName, ? extends Metric> metrics = consumer.metrics();
            System.out.println("metrics: " + metrics);
            Assert.assertFalse(CollectionExtension.isEmpty(metrics));
        }
    }

    @Test
    public void partitionsFor() throws InterruptedException {
        partitionsFor((c, t) -> c.partitionsFor(t));
    }

    @Test
    public void partitionsForWithTimeout() throws InterruptedException {
        Duration timeout = Duration.ofMillis(_waitTimeout);
        partitionsFor((c, t) -> c.partitionsFor(t, timeout));
    }

    protected void partitionsFor(BiFunction<Consumer<String, String>, String, List<PartitionInfo>> partitionInfoFetcher)
            throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumer()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);

            _topics.forEach(t -> {
                List<PartitionInfo> partitionInfos = partitionInfoFetcher.apply(consumer, t);
                System.out.println("partitionInfos: " + partitionInfos);
                Assert.assertFalse(CollectionExtension.isEmpty(partitionInfos));
            });
        }
    }

    @Test
    public void listTopics() throws InterruptedException {
        listTopics(c -> c.listTopics());
    }

    @Test
    public void listTopicsWithTimeout() throws InterruptedException {
        Duration timeout = Duration.ofMillis(_waitTimeout);
        listTopics(c -> c.listTopics(timeout));
    }

    protected void listTopics(Function<Consumer<String, String>, Map<String, List<PartitionInfo>>> partitionInfoFetcher)
            throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumer()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);

            Map<String, List<PartitionInfo>> partitionInfoMap = partitionInfoFetcher.apply(consumer);
            System.out.println("partitionInfoMap: " + partitionInfoMap);
            Assert.assertFalse(CollectionExtension.isEmpty(partitionInfoMap));
        }
    }

    @Test
    public void pausePausedResume() throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumer()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);

            Set<TopicPartition> paused = consumer.paused();
            System.out.println("paused: " + paused);
            Assert.assertTrue(CollectionExtension.isEmpty(paused));

            consumer.pause(_topicPartitions);

            Thread.sleep(30 * 1000);

            paused = consumer.paused();
            System.out.println("paused: " + paused);
            Assert.assertEquals(_topicPartitions, new HashSet<>(paused));

            produceMessages();

            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(30));
            System.out.println("consumerRecords: " + consumerRecords);
            Assert.assertEquals(0, consumerRecords.count());

            consumer.resume(_topicPartitions);

            pollDurationTimeout(consumer);
        }
    }

    @Test
    public void offsetsForTimes() throws InterruptedException {
        offsetsForTimes((c, m) -> c.offsetsForTimes(m));
    }

    @Test
    public void offsetsForTimesWithTimeout() throws InterruptedException {
        Duration timeout = Duration.ofMillis(_waitTimeout);
        offsetsForTimes((c, m) -> c.offsetsForTimes(m, timeout));
    }

    protected void offsetsForTimes(
            BiFunction<Consumer<String, String>, Map<TopicPartition, Long>, Map<TopicPartition, OffsetAndTimestamp>> offsetsForTimesFetcher)
            throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumer()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);

            Map<TopicPartition, Long> topicPartitionTimes = new HashMap<>();
            long time = System.currentTimeMillis() - _sendInterval * 5;
            _topicPartitions.forEach(tp -> {
                topicPartitionTimes.put(tp, time);
            });
            Map<TopicPartition, OffsetAndTimestamp> results = offsetsForTimesFetcher.apply(consumer,
                    topicPartitionTimes);
            System.out.println("results: " + results);
            Assert.assertFalse(CollectionExtension.isEmpty(results));
        }
    }

    @Test
    public void beginningOffsets() throws InterruptedException {
        beginningOffsets((c, m) -> c.beginningOffsets(m));
    }

    @Test
    public void beginningOffsetsWithTimeout() throws InterruptedException {
        Duration timeout = Duration.ofMillis(_waitTimeout);
        beginningOffsets((c, m) -> c.beginningOffsets(m, timeout));
    }

    protected void beginningOffsets(
            BiFunction<Consumer<String, String>, Collection<TopicPartition>, Map<TopicPartition, Long>> offsetsFetcher)
            throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumer()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);

            Map<TopicPartition, Long> results = offsetsFetcher.apply(consumer, _topicPartitions);
            System.out.println("results: " + results);
            Assert.assertFalse(CollectionExtension.isEmpty(results));
        }
    }

    @Test
    public void endOffsets() throws InterruptedException {
        endOffsets((c, m) -> c.endOffsets(m));
    }

    @Test
    public void endOffsetsWithTimeout() throws InterruptedException {
        Duration timeout = Duration.ofMillis(_waitTimeout);
        endOffsets((c, m) -> c.endOffsets(m, timeout));
    }

    protected void endOffsets(
            BiFunction<Consumer<String, String>, Collection<TopicPartition>, Map<TopicPartition, Long>> offsetsFetcher)
            throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumer()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);

            Map<TopicPartition, Long> results = offsetsFetcher.apply(consumer, _topicPartitions);
            System.out.println("results: " + results);
            Assert.assertFalse(CollectionExtension.isEmpty(results));
        }
    }

    @Test
    public void close() throws InterruptedException {
        close(c -> c.close());
    }

    @Test
    public void closeWithTimeout() throws InterruptedException {
        Duration timeout = Duration.ofMillis(_waitTimeout);
        close(c -> c.close(timeout));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void closeWithTimeoutDeprecated() throws InterruptedException {
        close(c -> c.close(_waitTimeout, TimeUnit.MILLISECONDS));
    }

    protected void close(java.util.function.Consumer<Consumer<String, String>> closer) throws InterruptedException {
        produceMessages();

        Consumer<String, String> consumer = createConsumer();
        try {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);
        } finally {
            closer.accept(consumer);
        }
    }

    @Test
    public void wakeup() throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumer()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);

            pollOut(consumer);

            CountDownLatch latch = new CountDownLatch(1);
            Thread thread = new Thread(() -> {
                try {
                    consumer.poll(Duration.ofSeconds(30));
                    latch.countDown();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            });
            thread.setDaemon(true);
            thread.start();

            boolean waitSuccess = latch.await(10, TimeUnit.SECONDS);
            System.out.println("waitSuccess: " + waitSuccess);
            Assert.assertFalse(waitSuccess);

            consumer.wakeup();

            latch.await();
        }
    }

    protected void pollOut(Consumer<String, String> consumer) {
        long beforePoll = System.currentTimeMillis();
        while (true) {
            ConsumerRecords<String, String> polled = consumer.poll(Duration.ofSeconds(1));
            System.out.println("polled count: " + polled.count());
            if (System.currentTimeMillis() - beforePoll > 60 * 1000) {
                Assert.assertEquals(0, polled.count());
                break;
            }
        }
    }

    @Test
    public void multiThreadedAccess() throws InterruptedException {
        produceMessages();

        try (Consumer<String, String> consumer = createConsumer()) {
            consumer.subscribe(_topics);
            pollDurationTimeout(consumer);

            pollOut(consumer);

            Thread thread = new Thread(() -> {
                try {
                    consumer.poll(Duration.ofSeconds(20));
                } catch (InterruptException e) {

                } catch (Throwable e) {
                    e.printStackTrace();
                }
            });
            thread.setDaemon(true);
            thread.start();
            System.out.println("Thread state: " + thread.getState());

            Thread.sleep(10 * 1000);

            System.out.println("Thread state: " + thread.getState());
            Assert.assertEquals(Thread.State.WAITING, thread.getState());

            boolean exceptionGot = false;
            try {
                consumer.assignment();
                Assert.fail("exception required before");
            } catch (ConcurrentModificationException e) {
                exceptionGot = true;
                System.out.println("got ConcurrentModificationException");
            }

            Assert.assertTrue(exceptionGot);

            Thread.sleep(15 * 1000);
        }
    }

    protected Consumer<String, String> createConsumer() {
        Properties properties = new Properties();
        properties.put("group.id", _consumerGroup);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "earliest");
        return getClientFactory().newConsumer(properties);
    }

    protected Consumer<String, String> createConsumerWithoutAutoCommit() {
        Properties properties = new Properties();
        properties.put("group.id", _consumerGroup);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "earliest");
        properties.put("enable.auto.commit", "false");
        return getClientFactory().newConsumer(properties);
    }

    protected void produceMessages() throws InterruptedException {
        Properties properties = new Properties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        try (Producer<String, String> producer = getClientFactory().newProducer(properties)) {
            ExecutorService executorService = Executors.newFixedThreadPool(_topics.size());
            AtomicInteger failedCount = new AtomicInteger();
            CountDownLatch latch = new CountDownLatch(_topics.size());
            try {
                _topics.forEach(t -> {
                    executorService.submit(() -> {
                        try {
                            produceMessages(producer, t);
                        } catch (Throwable e) {
                            failedCount.incrementAndGet();
                            System.out.println("produce messages failed for " + t);
                            e.printStackTrace();
                        } finally {
                            latch.countDown();
                        }
                    });
                });
            } finally {
                executorService.shutdown();
            }

            if (!latch.await(_waitTimeout, TimeUnit.MILLISECONDS))
                Assert.fail("produce message timeout: " + _waitTimeout);

            Assert.assertEquals(0, failedCount.get());
        }
    }

    protected void produceMessages(Producer<String, String> producer, String topic) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(_messageCount);
        AtomicBoolean failed = new AtomicBoolean();
        for (int i = 0; i < _messageCount; i++) {
            String v = String.valueOf(i);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, v, v);
            producer.send(record, (m, e) -> {
                latch.countDown();

                if (e != null) {
                    System.err.printf("\nrecord: %s, error: %s\n", record, e);
                    failed.set(true);
                } else
                    System.out.printf("\nrecord: %s, success\n", record, e);
            });
            System.out.printf("\nrecord: %s, sent\n", record);

            if (_sendInterval > 0)
                Thread.sleep(_sendInterval);
        }

        if (!latch.await(_waitTimeout, TimeUnit.MILLISECONDS))
            Assert.fail("send message timeout: " + _waitTimeout);

        Assert.assertFalse(failed.get());
    }

    @SuppressWarnings("deprecation")
    protected void pollLongTimeout(Consumer<String, String> consumer) {
        poll(() -> consumer.poll(_pollTimeout));
    }

    protected void pollDurationTimeout(Consumer<String, String> consumer) {
        Duration pollTimeout2 = Duration.ofMillis(_pollTimeout);
        poll(() -> consumer.poll(pollTimeout2));
    }

    protected void poll(Supplier<ConsumerRecords<String, String>> poller) {
        long onStart = System.currentTimeMillis();
        Map<String, AtomicInteger> actualTopicMessageCount = new HashMap<>();
        while (true) {
            ConsumerRecords<String, String> records = poller.get();
            records.forEach(r -> {
                actualTopicMessageCount.computeIfAbsent(r.topic(), k -> new AtomicInteger()).incrementAndGet();
                System.out.printf("\nrecord: %s\n", r);
            });

            AtomicBoolean hasInComplete = new AtomicBoolean();
            _topics.forEach(t -> {
                AtomicInteger actualCount = actualTopicMessageCount.computeIfAbsent(t, k -> new AtomicInteger());
                if (actualCount.get() < _messageCount)
                    hasInComplete.set(true);
            });

            if (!hasInComplete.get())
                break;

            long timeElipsed = System.currentTimeMillis() - onStart;
            if (timeElipsed >= _waitTimeout)
                Assert.fail("consume timeout, only consumed: " + actualTopicMessageCount + " messages, expected: "
                        + _messageCount);
        }
    }

}
