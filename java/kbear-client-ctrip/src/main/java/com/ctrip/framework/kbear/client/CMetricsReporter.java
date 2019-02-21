package com.ctrip.framework.kbear.client;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.mydotey.java.ObjectExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.framework.clogging.agent.metrics.IMetric;
import com.ctrip.framework.clogging.agent.metrics.MetricManager;

/**
 * @author koqizhao
 *
 * Jan 10, 2019
 */
@SuppressWarnings("rawtypes")
public class CMetricsReporter implements AutoCloseable {

    private static Logger _logger = LoggerFactory.getLogger(CMetricsReporter.class);

    private static IMetric _metric = MetricManager.getMetricer();

    private Map<String, String> _defaultTags;

    private List<WeakReference<Producer>> _producers;
    private List<WeakReference<Consumer>> _consumers;

    private ScheduledExecutorService _scheduledExecutorService;

    public CMetricsReporter(Map<String, String> defaultTags) {
        _defaultTags = defaultTags == null ? new HashMap<>() : new HashMap<>(defaultTags);

        _producers = new CopyOnWriteArrayList<>();
        _consumers = new CopyOnWriteArrayList<>();

        _scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            thread.setName("kafka-metrics-reporter");
            return thread;
        });
        _scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                report();
            } catch (Exception e) {
                _logger.error("report kafka metrics failed", e);
            }
        }, 60 * 1000, 60 * 1000, TimeUnit.MILLISECONDS);
    }

    public synchronized void add(Producer producer) {
        ObjectExtension.requireNonNull(producer, "producer");
        _producers.add(new WeakReference<>(producer));
    }

    public synchronized void add(Consumer consumer) {
        ObjectExtension.requireNonNull(consumer, "consumer");
        _consumers.add(new WeakReference<>(consumer));
    }

    protected void report() {
        Map<MetricName, Metric> metrics = getAllMetrics();
        HashMap<String, String> tags = new HashMap<>();
        metrics.forEach((n, m) -> {
            Double metricValue = getMetricValue(m);
            if (metricValue == null)
                return;

            tags.clear();
            tags.putAll(_defaultTags);
            tags.put("metricGroup", n.group());
            tags.putAll(n.tags());
            String clientId = n.tags().get("client-id");
            addClientIdTags(tags, clientId);
            String metricName = toMetricName(n);
            if (metricValue.doubleValue() > Float.MAX_VALUE)
                _metric.log(metricName, metricValue.longValue(), tags);
            else
                _metric.log(metricName, metricValue.floatValue(), tags);
        });
    }

    protected Double getMetricValue(Metric metric) {
        Object metricValue = metric.metricValue();
        if (metricValue == null)
            return null;

        if (metricValue instanceof Number)
            return ((Number) metricValue).doubleValue();

        return null;
    }

    protected void addClientIdTags(Map<String, String> tags, String clientId) {
        String[] parts = clientId.split("_");
        if (parts.length >= 2) {
            tags.put("topic", parts[1]);
            if (parts.length >= 3)
                tags.put("consumerGroup", parts[2]);
        }
    }

    protected String toMetricName(MetricName metricName) {
        String name = "kafka.";
        if (metricName.group() != null) {
            if (metricName.group().contains("producer"))
                name = "kafka.producer.";
            else if (metricName.group().contains("consumer"))
                name = "kafka.consumer.";
        }
        name += metricName.name();
        return name;
    }

    @SuppressWarnings("unchecked")
    protected synchronized Map<MetricName, Metric> getAllMetrics() {
        Map<MetricName, Metric> metrics = new HashMap<>();

        List<WeakReference> gced = new ArrayList<>();
        for (WeakReference<Producer> r : _producers) {
            Producer producer = r.get();
            if (producer == null) {
                gced.add(r);
                continue;
            }
            metrics.putAll(producer.metrics());
        }

        gced.forEach(_producers::remove);
        gced.clear();

        for (WeakReference<Consumer> r : _consumers) {
            Consumer consumer = r.get();
            if (consumer == null) {
                gced.add(r);
                continue;
            }
            metrics.putAll(consumer.metrics());
        }

        gced.forEach(_consumers::remove);

        return metrics;
    }

    @Override
    public void close() throws Exception {
        _scheduledExecutorService.shutdown();
    }

}
