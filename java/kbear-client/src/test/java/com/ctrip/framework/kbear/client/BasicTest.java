package com.ctrip.framework.kbear.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * @author koqizhao
 *
 * Jan 23, 2019
 */
@RunWith(Parameterized.class)
public class BasicTest extends KbearTestBase {

    @Parameters(name = "{index}: topics={0}, messageCount={1}, consumerGroup={2}, waitTimeout={3}, sendInterval={4}, pollTimeout={5}")
    public static Collection<Object[]> data() throws NoSuchMethodException, SecurityException {
        List<Object[]> parameterValues = new ArrayList<>();
        List<String> topics;
        Map<String, Integer> topicMessageCount;
        String consumerGroup;
        long waitTimeout;
        long sendInterval;
        long pollTimeout;

        topics = Arrays.asList(TestData.TOPIC);
        topicMessageCount = new HashMap<>();
        topicMessageCount.put(TestData.TOPIC, 10);
        consumerGroup = TestData.CONSUMER_GROUP;
        waitTimeout = 60 * 1000;
        sendInterval = 0;
        pollTimeout = 1 * 1000;
        parameterValues
                .add(new Object[] { topics, topicMessageCount, consumerGroup, waitTimeout, sendInterval, pollTimeout });

        topics = Arrays.asList(TestData.TOPIC, TestData.TOPIC_2);
        topicMessageCount = new HashMap<>();
        topicMessageCount.put(TestData.TOPIC, 10);
        topicMessageCount.put(TestData.TOPIC_2, 20);
        consumerGroup = TestData.CONSUMER_GROUP;
        waitTimeout = 2 * 60 * 1000;
        sendInterval = 100;
        pollTimeout = 1 * 1000;
        parameterValues
                .add(new Object[] { topics, topicMessageCount, consumerGroup, waitTimeout, sendInterval, pollTimeout });

        topics = Arrays.asList(TestData.TOPIC, TestData.TOPIC_2, TestData.TOPIC_3);
        topicMessageCount = new HashMap<>();
        topicMessageCount.put(TestData.TOPIC, 10);
        topicMessageCount.put(TestData.TOPIC_2, 20);
        topicMessageCount.put(TestData.TOPIC_3, 30);
        consumerGroup = TestData.CONSUMER_GROUP;
        waitTimeout = 3 * 60 * 1000;
        sendInterval = 10;
        pollTimeout = 100;
        parameterValues
                .add(new Object[] { topics, topicMessageCount, consumerGroup, waitTimeout, sendInterval, pollTimeout });

        topics = Arrays.asList(TestData.TOPIC, TestData.TOPIC_2, TestData.TOPIC_3);
        topicMessageCount = new HashMap<>();
        topicMessageCount.put(TestData.TOPIC, 10);
        topicMessageCount.put(TestData.TOPIC_2, 20);
        topicMessageCount.put(TestData.TOPIC_3, 30);
        consumerGroup = TestData.CONSUMER_GROUP_2;
        waitTimeout = 3 * 60 * 1000;
        sendInterval = 10;
        pollTimeout = 100;
        parameterValues
                .add(new Object[] { topics, topicMessageCount, consumerGroup, waitTimeout, sendInterval, pollTimeout });

        topics = Arrays.asList(TestData.TOPIC, TestData.TOPIC_2, TestData.TOPIC_3);
        topicMessageCount = new HashMap<>();
        topicMessageCount.put(TestData.TOPIC, 10);
        topicMessageCount.put(TestData.TOPIC_2, 20);
        topicMessageCount.put(TestData.TOPIC_3, 30);
        consumerGroup = "consumer-" + System.currentTimeMillis();
        waitTimeout = 3 * 60 * 1000;
        sendInterval = 10;
        pollTimeout = 100;
        parameterValues
                .add(new Object[] { topics, topicMessageCount, consumerGroup, waitTimeout, sendInterval, pollTimeout });

        return parameterValues;
    }

    @Parameter(0)
    public List<String> topics;

    @Parameter(1)
    public Map<String, Integer> topicMessageCount;

    @Parameter(2)
    public String consumerGroup;

    @Parameter(3)
    public long waitTimeout;

    @Parameter(4)
    public long sendInterval;

    @Parameter(5)
    public long pollTimeout;

    @Test
    public void produceConsume() throws InterruptedException {
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
                            produceMessages(producer, t, topicMessageCount.get(t), waitTimeout, sendInterval);
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

            if (!latch.await(waitTimeout, TimeUnit.MILLISECONDS))
                Assert.fail("produce message timeout: " + waitTimeout);

            Assert.assertEquals(0, failedCount.get());
        }

        properties = new Properties();
        properties.put("group.id", consumerGroup);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "earliest");
        try (Consumer<String, String> consumer = getClientFactory().newConsumer(properties)) {
            consumer.subscribe(topics);
            consumeMessages(consumer, topicMessageCount, waitTimeout, pollTimeout);
        }
    }

    protected void produceMessages(Producer<String, String> producer, String topic, int messageCount, long waitTimeout,
            long sendInterval) throws InterruptedException {
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

        if (!latch.await(waitTimeout, TimeUnit.MILLISECONDS))
            Assert.fail("send message timeout: " + waitTimeout);

        Assert.assertFalse(failed.get());
    }

    protected void consumeMessages(Consumer<String, String> consumer, Map<String, Integer> topicMessageCount,
            long waitTimeout, long pollTimeout) {
        long onStart = System.currentTimeMillis();
        Duration pollTimeout2 = Duration.ofMillis(pollTimeout);
        Map<String, AtomicInteger> actualTopicMessageCount = new HashMap<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(pollTimeout2);
            records.forEach(r -> {
                actualTopicMessageCount.computeIfAbsent(r.topic(), k -> new AtomicInteger()).incrementAndGet();
                System.out.printf("\nrecord: %s\n", r);
            });

            AtomicBoolean hasInComplete = new AtomicBoolean();
            topicMessageCount.forEach((t, c) -> {
                AtomicInteger actualCount = actualTopicMessageCount.computeIfAbsent(t, k -> new AtomicInteger());
                if (actualCount.get() < c)
                    hasInComplete.set(true);
            });

            if (!hasInComplete.get())
                break;

            long timeElipsed = System.currentTimeMillis() - onStart;
            if (timeElipsed >= waitTimeout)
                Assert.fail("consume timeout, only consumed: " + actualTopicMessageCount + " messages, expected: "
                        + topicMessageCount);
        }
    }

}
