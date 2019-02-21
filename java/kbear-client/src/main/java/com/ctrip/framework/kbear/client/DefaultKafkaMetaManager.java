package com.ctrip.framework.kbear.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.mydotey.codec.json.JacksonJsonCodec;
import org.mydotey.java.CloseableExtension;
import org.mydotey.java.ObjectExtension;
import org.mydotey.java.collection.CollectionExtension;
import org.mydotey.rpc.client.http.HttpLoadBalancer;
import org.mydotey.rpc.client.http.HttpServiceClientConfig;
import org.mydotey.rpc.client.http.RandomLoadBalancer;
import org.mydotey.rpc.client.http.apache.async.DynamicPoolingNHttpClientProvider;
import org.mydotey.rpc.client.http.apache.sync.DynamicPoolingHttpClientProvider;
import org.mydotey.scf.ConfigurationManager;
import org.mydotey.scf.Property;
import org.mydotey.scf.PropertyChangeEvent;
import org.mydotey.scf.facade.StringProperties;
import org.mydotey.scf.type.string.StringInplaceConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.framework.kbear.meta.Cluster;
import com.ctrip.framework.kbear.meta.ConsumerGroup;
import com.ctrip.framework.kbear.meta.ConsumerGroupId;
import com.ctrip.framework.kbear.meta.Topic;
import com.ctrip.framework.kbear.route.Client;
import com.ctrip.framework.kbear.route.FetchConsumerRouteRequest;
import com.ctrip.framework.kbear.route.FetchConsumerRouteResponse;
import com.ctrip.framework.kbear.route.FetchProducerRouteRequest;
import com.ctrip.framework.kbear.route.FetchProducerRouteResponse;
import com.ctrip.framework.kbear.route.Route;
import com.ctrip.framework.kbear.route.RouteServiceClient;

/**
 * @author koqizhao
 *
 * Dec 22, 2018
 */
public class DefaultKafkaMetaManager implements KafkaMetaManager {

    private static Logger _logger = LoggerFactory.getLogger(DefaultKafkaMetaManager.class);

    private Client _client;

    private ConfigurationManager _configurationManager;
    private Property<String, List<String>> _metaServiceUrls;
    private Property<String, Integer> _metaUpdateInterval;
    private Property<String, Integer> _metaUpdateRetryTimes;
    private Property<String, Integer> _metaUpdateRetryInterval;

    private ConcurrentHashMap<String, Runnable> _producerListeners;
    private ConcurrentHashMap<ConsumerGroupId, Runnable> _consumerListeners;
    private volatile KafkaMetaHolder _metaHolder;

    private volatile boolean _closed;
    private Thread _metaUpdater;
    private volatile RouteServiceClient _routeServiceClient;

    public DefaultKafkaMetaManager(ConfigurationManager configurationManager) {
        this(configurationManager, null);
    }

    public DefaultKafkaMetaManager(ConfigurationManager configurationManager, Client client) {
        ObjectExtension.requireNonNull(configurationManager, "configurationManager");
        _configurationManager = configurationManager;
        _client = client == null ? new Client() : client.clone();

        StringProperties stringProperties = new StringProperties(_configurationManager);
        String key = "kafka.meta.service.url";
        _metaServiceUrls = stringProperties.getListProperty(key, null, StringInplaceConverter.DEFAULT,
                v -> CollectionExtension.isEmpty(v) ? null : v);
        if (CollectionExtension.isEmpty(_metaServiceUrls.getValue()))
            throw new IllegalArgumentException("property is not configured: " + key);
        _metaUpdateInterval = stringProperties.getIntProperty("kafka.meta.update.interval", 60 * 1000,
                v -> v < 0 ? null : v);
        _metaUpdateRetryTimes = stringProperties.getIntProperty("kafka.meta.update.retry.times", 3,
                v -> v < 1 ? null : v);
        _metaUpdateRetryInterval = stringProperties.getIntProperty("kafka.meta.update.retry.interval", 10,
                v -> v < 0 ? null : v);

        _producerListeners = new ConcurrentHashMap<>();
        _consumerListeners = new ConcurrentHashMap<>();
        _metaHolder = new KafkaMetaHolder();
        _metaHolder.immutable();

        _routeServiceClient = new RouteServiceClient(newServiceClientConfig(_metaServiceUrls.getValue()));
        _metaServiceUrls.addChangeListener(this::updateRouteServiceClient);

        _metaUpdater = new Thread(this::updateMeta, "kafka.meta.update.executor");
        _metaUpdater.setDaemon(true);
        _metaUpdater.start();
    }

    protected ConfigurationManager getConfigurationManager() {
        return _configurationManager;
    }

    protected Client getClient() {
        return _client;
    }

    @Override
    public KafkaMetaHolder getMetaHolder() {
        return _metaHolder;
    }

    @Override
    public synchronized void registerProducer(String topicId, Runnable onChange) {
        ObjectExtension.requireNonNull(topicId, "topicId");
        ObjectExtension.requireNonNull(onChange, "onChange");

        _producerListeners.put(topicId, onChange);
        if (!_metaHolder.getTopicRoutes().containsKey(topicId))
            trySync(() -> syncProducer(topicId));
    }

    @Override
    public synchronized void unregisterProducer(String topicId) {
        ObjectExtension.requireNonNull(topicId, "topicId");

        _producerListeners.remove(topicId);
        KafkaMetaHolder metaHolder = _metaHolder.clone();
        metaHolder.getTopicRoutes().remove(topicId);
        metaHolder.immutable();
        _metaHolder = metaHolder;
    }

    @Override
    public synchronized void registerConsumer(ConsumerGroupId consumerGroupId, Runnable onChange) {
        ObjectExtension.requireNonNull(consumerGroupId, "consumerGroupId");
        ObjectExtension.requireNonNull(onChange, "onChange");

        _consumerListeners.put(consumerGroupId.clone(), onChange);
        if (!_metaHolder.getConsumerGroupRoutes().containsKey(consumerGroupId))
            trySync(() -> syncConsumer(consumerGroupId));
    }

    @Override
    public synchronized void unregisterConsumer(ConsumerGroupId consumerGroupId) {
        ObjectExtension.requireNonNull(consumerGroupId, "consumerGroupId");

        _consumerListeners.remove(consumerGroupId);
        KafkaMetaHolder metaHolder = _metaHolder.clone();
        metaHolder.getConsumerGroups().remove(consumerGroupId);
        metaHolder.getConsumerGroupRoutes().remove(consumerGroupId);
        metaHolder.immutable();
        _metaHolder = metaHolder;
    }

    protected void trySync(Supplier<Boolean> syncAction) {
        for (int i = 0; i < _metaUpdateRetryTimes.getValue();) {
            if (syncAction.get())
                break;

            i++;
            if (i < _metaUpdateRetryTimes.getValue()) {
                try {
                    Thread.sleep(_metaUpdateRetryInterval.getValue());
                } catch (InterruptedException e) {
                    _logger.warn("retry interrupted, ignore", e);
                    break;
                }

                _logger.info("retry at time {}", i);
            }
        }
    }

    protected void updateMeta() {
        while (!_closed) {
            try {
                Thread.sleep(_metaUpdateInterval.getValue());

                synchronized (this) {
                    doUpdateMeta();
                }
            } catch (InterruptedException e) {
                break;
            } catch (Exception e) {
                _logger.error("meta update failed", e);
            }
        }
    }

    protected void doUpdateMeta() {
        KafkaMetaHolder metaHolder = new KafkaMetaHolder();

        if (_producerListeners.size() > 0) {
            FetchProducerRouteRequest fetchProducerRouteRequest = new FetchProducerRouteRequest();
            fetchProducerRouteRequest.setClient(_client);
            fetchProducerRouteRequest.setTopicIds(new ArrayList<>(_producerListeners.keySet()));
            FetchProducerRouteResponse fetchProducerRouteResponse = _routeServiceClient
                    .fetchProducerRoute(fetchProducerRouteRequest);
            fetchProducerRouteResponse.getTopicIdRoutes().forEach(metaHolder.getTopicRoutes()::put);
            fetchProducerRouteResponse.getClusters().forEach(metaHolder.getClusters()::put);
            fetchProducerRouteResponse.getTopics().forEach(metaHolder.getTopics()::put);
        }

        if (_consumerListeners.size() > 0) {
            FetchConsumerRouteRequest fetchConsumerRouteRequest = new FetchConsumerRouteRequest();
            fetchConsumerRouteRequest.setClient(_client);
            fetchConsumerRouteRequest.setConsumerGroupIds(new ArrayList<>(_consumerListeners.keySet()));
            FetchConsumerRouteResponse fetchConsumerRouteResponse = _routeServiceClient
                    .fetchConsumerRoute(fetchConsumerRouteRequest);
            fetchConsumerRouteResponse.getConsumerGroupIdRoutes()
                    .forEach(p -> metaHolder.getConsumerGroupRoutes().put(p.getConsumerGroupId(), p.getRoute()));
            fetchConsumerRouteResponse.getConsumerGroups()
                    .forEach(c -> metaHolder.getConsumerGroups().put(c.getId(), c));
            fetchConsumerRouteResponse.getClusters().forEach(metaHolder.getClusters()::put);
            fetchConsumerRouteResponse.getTopics().forEach(metaHolder.getTopics()::put);

        }

        metaHolder.immutable();
        updateMetaHolder(metaHolder);
    }

    protected boolean syncProducer(String topicId) {
        try {
            FetchProducerRouteRequest request = new FetchProducerRouteRequest();
            request.setClient(_client);
            request.setTopicIds(Arrays.asList(topicId));
            FetchProducerRouteResponse response = _routeServiceClient.fetchProducerRoute(request);
            KafkaMetaHolder metaHolder = _metaHolder.clone();
            response.getTopicIdRoutes().forEach(metaHolder.getTopicRoutes()::put);
            response.getClusters().forEach(metaHolder.getClusters()::put);
            response.getTopics().forEach(metaHolder.getTopics()::put);

            metaHolder.immutable();
            _metaHolder = metaHolder;
            return true;
        } catch (Exception e) {
            _logger.error("sync producer failed", e);
            return false;
        }
    }

    protected boolean syncConsumer(ConsumerGroupId consumerGroupId) {
        try {
            FetchConsumerRouteRequest request = new FetchConsumerRouteRequest();
            request.setClient(_client);
            request.setConsumerGroupIds(Arrays.asList(consumerGroupId));
            FetchConsumerRouteResponse response = _routeServiceClient.fetchConsumerRoute(request);
            KafkaMetaHolder metaHolder = _metaHolder.clone();
            response.getConsumerGroupIdRoutes()
                    .forEach(p -> metaHolder.getConsumerGroupRoutes().put(p.getConsumerGroupId(), p.getRoute()));
            response.getConsumerGroups().forEach(c -> metaHolder.getConsumerGroups().put(c.getId(), c));
            response.getClusters().forEach(metaHolder.getClusters()::put);
            response.getTopics().forEach(metaHolder.getTopics()::put);

            metaHolder.immutable();
            _metaHolder = metaHolder;
            return true;
        } catch (Exception e) {
            _logger.error("sync consumer failed", e);
            return false;
        }
    }

    protected void updateMetaHolder(KafkaMetaHolder newMetaHolder) {
        KafkaMetaHolder oldMetaHolder = _metaHolder;
        _metaHolder = newMetaHolder;

        _producerListeners.forEach((t, l) -> {
            if (isChanged(t, oldMetaHolder, newMetaHolder)) {
                try {
                    _logger.info("Topic metadata changed, notify listener to run: " + t);
                    l.run();
                } catch (Exception e) {
                    _logger.error("Topic metadata change listener failed to run: " + t, e);
                }
            }
        });

        _consumerListeners.forEach((c, l) -> {
            if (isChanged(c, oldMetaHolder, newMetaHolder)) {
                try {
                    _logger.info("Consumer metadata changed, notify listener to run: " + c);
                    l.run();
                } catch (Exception e) {
                    _logger.error("Consumer metadata change listener failed to run: " + c, e);
                }
            }
        });
    }

    protected boolean isChanged(String topicId, KafkaMetaHolder oldMetaHolder, KafkaMetaHolder newMetaHolder) {
        Topic oldTopic = oldMetaHolder.getTopics().get(topicId);
        Topic newTopic = newMetaHolder.getTopics().get(topicId);
        if (!Objects.equals(oldTopic, newTopic)) {
            _logger.info("Topic {} changed from {} to {}", topicId, oldTopic, newTopic);
            return true;
        }

        Route oldRoute = oldMetaHolder.getTopicRoutes().get(topicId);
        Route newRoute = newMetaHolder.getTopicRoutes().get(topicId);
        if (!Objects.equals(oldRoute, newRoute)) {
            _logger.info("Route for {} changed from {} to {}", topicId, oldRoute, newRoute);
            return true;
        }

        Cluster oldCluster = oldMetaHolder.getClusters().get(oldRoute.getClusterId());
        Cluster newCluster = newMetaHolder.getClusters().get(newRoute.getClusterId());
        if (!Objects.equals(oldCluster, newCluster)) {
            _logger.info("Cluster for {} changed from {} to {}", topicId, oldCluster, newCluster);
            return true;
        }

        return false;
    }

    protected boolean isChanged(ConsumerGroupId consumerGroupId, KafkaMetaHolder oldMetaHolder,
            KafkaMetaHolder newMetaHolder) {
        ConsumerGroup oldConsumerGroup = oldMetaHolder.getConsumerGroups().get(consumerGroupId);
        ConsumerGroup newConsumerGrop = newMetaHolder.getConsumerGroups().get(consumerGroupId);
        if (!Objects.equals(oldConsumerGroup, newConsumerGrop)) {
            _logger.info("ConsumerGroup {} changed from {} to {}", consumerGroupId, oldConsumerGroup, newConsumerGrop);
            return true;
        }

        Route oldRoute = oldMetaHolder.getConsumerGroupRoutes().get(consumerGroupId);
        Route newRoute = newMetaHolder.getConsumerGroupRoutes().get(consumerGroupId);
        if (!Objects.equals(oldRoute, newRoute)) {
            _logger.info("Route for {} changed from {} to {}", consumerGroupId, oldRoute, newRoute);
            return true;
        }

        Cluster oldCluster = oldMetaHolder.getClusters().get(oldRoute.getClusterId());
        Cluster newCluster = newMetaHolder.getClusters().get(newRoute.getClusterId());
        if (!Objects.equals(oldCluster, newCluster)) {
            _logger.info("Cluster for {} changed from {} to {}", consumerGroupId, oldCluster, newCluster);
            return true;
        }

        return false;
    }

    protected HttpServiceClientConfig newServiceClientConfig(List<String> serviceUrls) {
        DynamicPoolingHttpClientProvider syncClientProvider = new DynamicPoolingHttpClientProvider(
                "kafka.meta.route.client", _configurationManager);
        DynamicPoolingNHttpClientProvider asyncClientProvider = new DynamicPoolingNHttpClientProvider(
                "kafka.meta.route.async-client", _configurationManager);
        return new HttpServiceClientConfig.Builder().setProcedureRestPathMap(RouteServiceClient.PROCEDURE_REST_PATH_MAP)
                .setSyncClientProvider(syncClientProvider).setAsyncClientProvider(asyncClientProvider)
                .setCodec(JacksonJsonCodec.DEFAULT).setLoadBalancer(newLoadBalancer(serviceUrls)).build();
    }

    protected void updateRouteServiceClient(PropertyChangeEvent<String, List<String>> e) {
        RouteServiceClient routeServiceClient = _routeServiceClient;
        HttpServiceClientConfig oldConfig = routeServiceClient.getConfig();
        HttpServiceClientConfig newConfig = new HttpServiceClientConfig.Builder()
                .setProcedureRestPathMap(oldConfig.getProcedureRestPathMap())
                .setSyncClientProvider(oldConfig.getSyncClientProvider())
                .setAsyncClientProvider(oldConfig.getAsyncClientProvider()).setCodec(oldConfig.getCodec())
                .setLoadBalancer(newLoadBalancer(e.getNewValue())).build();
        _routeServiceClient = new RouteServiceClient(newConfig);

        CloseableExtension.close(routeServiceClient);

        synchronized (this) {
            doUpdateMeta();
        }
    }

    protected HttpLoadBalancer newLoadBalancer(List<String> serviceUrls) {
        return new RandomLoadBalancer(serviceUrls, 5 * 60 * 1000, 10 * 1000);
    }

    @Override
    public void close() throws Exception {
        _closed = true;
        _metaUpdater.interrupt();
        CloseableExtension.close(_routeServiceClient);
        CloseableExtension.close((AutoCloseable) _routeServiceClient.getConfig().getSyncClientProvider());
        CloseableExtension.close((AutoCloseable) _routeServiceClient.getConfig().getAsyncClientProvider());
    }

}
