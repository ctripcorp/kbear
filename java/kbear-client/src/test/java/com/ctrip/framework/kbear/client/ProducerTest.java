package com.ctrip.framework.kbear.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mydotey.java.collection.CollectionExtension;

/**
 * @author koqizhao
 *
 * Jan 23, 2019
 */
@RunWith(Parameterized.class)
public class ProducerTest extends KbearTestBase {

    @Parameters(name = "{index}: topics={0}, messageCount={1}, sendInterval={2}")
    public static Collection<Object[]> data() throws NoSuchMethodException, SecurityException {
        List<Object[]> parameterValues = new ArrayList<>();
        List<String> topics;
        int messageCount;
        long sendInterval;

        topics = Arrays.asList(TestData.TOPIC_6);
        messageCount = 10;
        sendInterval = 100;
        parameterValues.add(new Object[] { topics, messageCount, sendInterval });

        topics = Arrays.asList(TestData.TOPIC_7);
        messageCount = 100;
        sendInterval = 0;
        parameterValues.add(new Object[] { topics, messageCount, sendInterval });

        topics = Arrays.asList(TestData.TOPIC_6, TestData.TOPIC_7);
        messageCount = 100;
        sendInterval = 0;
        parameterValues.add(new Object[] { topics, messageCount, sendInterval });

        topics = Arrays.asList(TestData.TOPIC_6, TestData.TOPIC_7, TestData.TOPIC_8);
        messageCount = 10;
        sendInterval = 100;
        parameterValues.add(new Object[] { topics, messageCount, sendInterval });

        return parameterValues;
    }

    @Parameter(0)
    public List<String> topics;

    @Parameter(1)
    public int messageCount;

    @Parameter(2)
    public long sendInterval;

    private long _waitTimeout = 10 * 60 * 1000;

    @Test
    public void produceWithCallback() throws InterruptedException {
        Properties properties = new Properties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        try (Producer<String, String> producer = getClientFactory().newProducer(properties)) {
            ExecutorService executorService = Executors.newFixedThreadPool(topics.size());
            AtomicInteger failedCount = new AtomicInteger();
            CountDownLatch latch = new CountDownLatch(topics.size());
            try {
                topics.forEach(t -> {
                    executorService.submit(() -> {
                        try {
                            produceMessagesWithCallback(producer, t);
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

            checkOtherApis(producer);
        }
    }

    @Test
    public void produceWithoutCallback() throws InterruptedException {
        Properties properties = new Properties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        try (Producer<String, String> producer = getClientFactory().newProducer(properties)) {
            ExecutorService executorService = Executors.newFixedThreadPool(topics.size());
            AtomicInteger failedCount = new AtomicInteger();
            CountDownLatch latch = new CountDownLatch(topics.size());
            try {
                topics.forEach(t -> {
                    executorService.submit(() -> {
                        try {
                            produceMessagesWithoutCallback(producer, t);
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

            checkOtherApis(producer);
        }
    }

    @Test
    public void close() throws InterruptedException {
        close(p -> p.close());
    }

    @Test
    public void closeWithTimeout() throws InterruptedException {
        close(p -> p.close(_waitTimeout, TimeUnit.MILLISECONDS));
    }

    protected void close(java.util.function.Consumer<Producer<String, String>> closer) throws InterruptedException {
        Properties properties = new Properties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = getClientFactory().newProducer(properties);
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(topics.size());
            AtomicInteger failedCount = new AtomicInteger();
            CountDownLatch latch = new CountDownLatch(topics.size());
            try {
                topics.forEach(t -> {
                    executorService.submit(() -> {
                        try {
                            produceMessagesWithoutCallback(producer, t);
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

            checkOtherApis(producer);
        } finally {
            closer.accept(producer);
        }
    }

    protected void produceMessagesWithCallback(Producer<String, String> producer, String topic)
            throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(messageCount);
        AtomicBoolean failed = new AtomicBoolean();
        for (int i = 0; i < messageCount; i++) {
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

            if (sendInterval > 0)
                Thread.sleep(sendInterval);
        }

        if (!latch.await(_waitTimeout, TimeUnit.MILLISECONDS))
            Assert.fail("send message timeout: " + _waitTimeout);

        Assert.assertFalse(failed.get());
    }

    protected void produceMessagesWithoutCallback(Producer<String, String> producer, String topic)
            throws InterruptedException {
        List<Future<RecordMetadata>> futures = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            String v = String.valueOf(i);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, v, v);
            Future<RecordMetadata> future = producer.send(record);
            futures.add(future);
            System.out.printf("\nrecord: %s, sent\n", record);

            if (sendInterval > 0)
                Thread.sleep(sendInterval);
        }

        long now = System.currentTimeMillis();
        futures.forEach(f -> {
            try {
                long timeout = _waitTimeout - (System.currentTimeMillis() - now);
                if (timeout <= 0)
                    timeout = 1;

                RecordMetadata recordMetadata = f.get(timeout, TimeUnit.MILLISECONDS);
                Assert.assertNotNull(recordMetadata);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                e.printStackTrace();
                Assert.fail(e.getMessage());
            }
        });
    }

    protected void checkOtherApis(Producer<String, String> producer) {
        topics.forEach(t -> {
            List<PartitionInfo> partitions = producer.partitionsFor(t);
            Assert.assertNotNull(partitions);
            Assert.assertEquals(1, partitions.size());
        });

        Map<MetricName, ?> metrics = producer.metrics();
        System.out.println("metrics: " + metrics);
        Assert.assertFalse(CollectionExtension.isEmpty(metrics));
    }

}
