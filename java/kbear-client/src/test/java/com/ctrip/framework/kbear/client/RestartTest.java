package com.ctrip.framework.kbear.client;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import org.mydotey.scf.ConfigurationManager;
import org.mydotey.scf.ConfigurationManagerConfig;
import org.mydotey.scf.facade.ConfigurationManagers;
import org.mydotey.scf.facade.StringPropertySources;
import org.mydotey.scf.source.stringproperty.memorymap.MemoryMapConfigurationSource;

/**
 * @author koqizhao
 *
 * Jan 23, 2019
 */
public class RestartTest extends KbearTestBase {

    private MemoryMapConfigurationSource _configurationSource;

    private List<String> _topics;

    private int _messageCount = 10;

    private long _waitTimeout = 10 * 60 * 1000;

    private long _sendInterval = 100;

    private long _pollTimeout = 100;

    private long _metaUpdateInverval = 20 * 1000;
    private long _restartSleep = _metaUpdateInverval + 5 * 1000;

    @Test
    public void restartProducer() throws InterruptedException {
        _topics = Arrays.asList(TestData.TOPIC, TestData.TOPIC_2, TestData.TOPIC_3, TestData.TOPIC_4);

        Properties producerProperties = new Properties();
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Properties consumerProperties = new Properties();
        consumerProperties.put("group.id", TestData.CONSUMER_GROUP);
        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put("auto.offset.reset", "earliest");

        Properties consumerProperties2 = new Properties();
        consumerProperties2.put("group.id", TestData.CONSUMER_GROUP_2);
        consumerProperties2.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties2.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties2.put("auto.offset.reset", "earliest");

        try (Producer<String, String> producer = getClientFactory().newProducer(producerProperties);
                Consumer<String, String> consumer = getClientFactory().newConsumer(consumerProperties);
                Consumer<String, String> consumer2 = getClientFactory().newConsumer(consumerProperties2);) {
            consumer.subscribe(_topics);
            consumer2.subscribe(_topics);

            produce(producer);
            consumeMessages(consumer, _messageCount, TestData.TOPIC_4, _messageCount);
            consumeMessages(consumer2, _messageCount, TestData.TOPIC_4, 0);

            _configurationSource.setPropertyValue(TestData.META_SERVICE_PROPERTY_KEY, TestData.META_SERVICE_URL_2);
            System.out.println("Sleep " + _restartSleep + "ms so as to change route");
            Thread.sleep(_restartSleep);
            produce(producer);
            consumeMessages(consumer, _messageCount, TestData.TOPIC_4, 0);
            consumeMessages(consumer2, _messageCount, TestData.TOPIC_4, _messageCount);

            _configurationSource.setPropertyValue(TestData.META_SERVICE_PROPERTY_KEY, TestData.META_SERVICE_URL);
            System.out.println("Sleep " + _restartSleep + "ms so as to change route");
            Thread.sleep(_restartSleep);
            produce(producer);
            consumeMessages(consumer, _messageCount, TestData.TOPIC_4, _messageCount);
            consumeMessages(consumer2, _messageCount, TestData.TOPIC_4, 0);
        }
    }

    @Test
    public void restartConsumer() throws InterruptedException {
        _topics = Arrays.asList(TestData.TOPIC, TestData.TOPIC_2, TestData.TOPIC_3, TestData.TOPIC_5);

        Properties producerProperties = new Properties();
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Properties consumerProperties = new Properties();
        consumerProperties.put("group.id", TestData.CONSUMER_GROUP);
        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put("auto.offset.reset", "earliest");

        try (Producer<String, String> producer = getClientFactory().newProducer(producerProperties);
                Consumer<String, String> consumer = getClientFactory().newConsumer(consumerProperties);) {
            consumer.subscribe(_topics);

            produce(producer);
            consumeMessages(consumer, _messageCount, TestData.TOPIC_5, _messageCount);

            _configurationSource.setPropertyValue(TestData.META_SERVICE_PROPERTY_KEY, TestData.META_SERVICE_URL_2);
            System.out.println("Sleep " + _restartSleep + "ms so as to change route");
            Thread.sleep(_restartSleep);
            produce(producer);
            consumeMessages(consumer, _messageCount, TestData.TOPIC_5, 0);

            _configurationSource.setPropertyValue(TestData.META_SERVICE_PROPERTY_KEY, TestData.META_SERVICE_URL);
            System.out.println("Sleep " + _restartSleep + "ms so as to change route");
            Thread.sleep(_restartSleep);
            consumeMessages(consumer, 0, TestData.TOPIC_5, _messageCount);
        }
    }

    protected void produce(Producer<String, String> producer) throws InterruptedException {
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

    protected void produceMessages(Producer<String, String> producer, String topic) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(_messageCount);
        AtomicBoolean failed = new AtomicBoolean();
        for (int i = 0; i < _messageCount; i++) {
            String v = String.valueOf(i);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, v, v);
            producer.send(record, (m, e) -> {
                if (e != null) {
                    System.err.printf("\nrecord: %s, error: %s\n", record, e);
                    failed.set(true);
                } else
                    System.out.printf("\nrecord: %s, success\n", record, e);

                latch.countDown();
            });
            System.out.printf("\nrecord: %s, sent\n", record);

            if (_sendInterval > 0)
                Thread.sleep(_sendInterval);
        }

        if (!latch.await(_waitTimeout, TimeUnit.MILLISECONDS))
            Assert.fail("send message timeout: " + _waitTimeout);

        Assert.assertFalse(failed.get());
    }

    protected void consumeMessages(Consumer<String, String> consumer, int defaultMessageCount, String topic,
            int topicMessageCount) {
        long onStart = System.currentTimeMillis();
        Duration pollTimeout2 = Duration.ofMillis(_pollTimeout);
        Map<String, AtomicInteger> actualTopicMessageCount = new HashMap<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(pollTimeout2);
            records.forEach(r -> {
                actualTopicMessageCount.computeIfAbsent(r.topic(), k -> new AtomicInteger()).incrementAndGet();
                System.out.printf("\nrecord: %s\n", r);
            });

            AtomicBoolean hasInComplete = new AtomicBoolean();
            _topics.forEach(t -> {
                AtomicInteger actualCount = actualTopicMessageCount.computeIfAbsent(t, k -> new AtomicInteger());
                int messageCount = Objects.equals(t, topic) ? topicMessageCount : defaultMessageCount;
                if (actualCount.get() < messageCount)
                    hasInComplete.set(true);
            });

            if (!hasInComplete.get())
                break;

            long timeElipsed = System.currentTimeMillis() - onStart;
            if (timeElipsed >= _waitTimeout)
                Assert.fail("consume timeout, only consumed: " + actualTopicMessageCount
                        + " messages, expected messageCount: " + _messageCount + ", expected " + topic
                        + " messageCount: " + topicMessageCount);
        }
    }

    @Override
    protected ConfigurationManager newConfigurationManager() {
        _configurationSource = StringPropertySources.newMemoryMapSource("memory");
        _configurationSource.setPropertyValue("kafka.meta.update.interval", String.valueOf(_metaUpdateInverval));
        _configurationSource.setPropertyValue(TestData.META_SERVICE_PROPERTY_KEY, TestData.META_SERVICE_URL);
        ConfigurationManagerConfig managerConfig = ConfigurationManagers.newConfigBuilder()
                .setName("kbear-kafka-client").addSource(1, _configurationSource).build();
        return ConfigurationManagers.newManager(managerConfig);
    }

}
