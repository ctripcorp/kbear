package com.ctrip.framework.kbear.hermes.proxy;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.mydotey.rpc.ack.Acks;
import org.mydotey.rpc.client.http.HttpConnectException;
import org.mydotey.rpc.client.http.HttpTimeoutException;
import org.mydotey.rpc.client.http.apache.ApacheHttpRequestException;
import org.mydotey.rpc.client.http.apache.HttpRequestFactory;
import org.mydotey.rpc.client.http.apache.sync.DynamicPoolingHttpClientProvider;
import org.mydotey.rpc.client.http.apache.sync.HttpRequestExecutors;
import org.mydotey.scf.ConfigurationManager;
import org.mydotey.scf.Property;
import org.mydotey.scf.facade.StringProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMethod;

import com.ctrip.framework.kbear.meta.Cluster;
import com.ctrip.framework.kbear.meta.repository.ClusterRepository;
import com.ctrip.framework.kbear.rest.CustomBeanConfiguration;
import com.ctrip.framework.kbear.rest.RestConfig;
import com.ctrip.framework.kbear.route.Client;
import com.ctrip.framework.kbear.route.FetchProducerRouteRequest;
import com.ctrip.framework.kbear.route.FetchProducerRouteResponse;
import com.ctrip.framework.kbear.route.Route;
import com.ctrip.framework.kbear.route.RouteService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

/**
 * @author koqizhao
 *
 * Dec 19, 2018
 */
@RequestMapping(path = "/hermes/topics", method = RequestMethod.POST, consumes = { MediaType.APPLICATION_JSON_VALUE,
        RestConfig.APPLICATION_PROTOBUF_VALUE })
@RestController
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TopicResource {

    private static Logger _logger = LoggerFactory.getLogger(TopicResource.class);

    private ObjectMapper _objectMapper = new ObjectMapper();

    @Inject
    @Named(CustomBeanConfiguration.BEAN_APP_CONFIG)
    private ConfigurationManager _configurationManager;

    @Inject
    private RouteService _routeService;

    private Property<String, String> _hermesPortalApiUrl;
    private DynamicPoolingHttpClientProvider _httpClientProvider;

    @PostConstruct
    private void init() {
        StringProperties properties = new StringProperties(_configurationManager);
        _hermesPortalApiUrl = properties.getStringProperty("hermes.portal.api.url");
        _httpClientProvider = new DynamicPoolingHttpClientProvider("hermes.proxy", _configurationManager);
    }

    @PreDestroy
    private void destroy() throws IOException {
        _httpClientProvider.close();
    }

    @RequestMapping(path = "/{topicId}", method = RequestMethod.GET, consumes = "*/*", produces = "*/*")
    public Object fetchTopic(@PathVariable("topicId") String topicId, HttpServletResponse response) {
        String url = getTopicApiUrl() + topicId;
        try {
            HttpUriRequest request = HttpRequestFactory.createRequest(url, HttpGet.METHOD_NAME);
            Map data = HttpRequestExecutors.execute(_httpClientProvider.get(), request, r -> {
                try {
                    return _objectMapper.readValue(r.getEntity().getContent(), Map.class);
                } catch (UnsupportedOperationException | IOException e) {
                    throw new RuntimeException(e);
                }
            });

            String storageType = (String) data.get("storageType");
            if (!Objects.equals(storageType, "kafka"))
                return data;

            FetchProducerRouteRequest request2 = FetchProducerRouteRequest.newBuilder().addTopicIds(topicId)
                    .setClient(Client.newBuilder().setId("kafka-hermes-proxy").build()).build();
            FetchProducerRouteResponse response2 = _routeService.fetchProducerRoute(request2);
            if (Acks.isFail(response2.getStatus().getAck())) {
                String message = "unknown error: " + response2.getStatus();
                _logger.error(message);
                response.setStatus(500);
                return message;
            }

            Route route = response2.getTopicIdRoutesMap().get(topicId);
            if (route == null)
                return data;

            Cluster cluster = response2.getClustersMap().get(route.getClusterId());
            String bootstrapServers = cluster.getMetaMap().get(ClusterRepository.KEY_BOOTSTRAP_SERVERS);
            String zookeeperConnect = cluster.getMetaMap().get(ClusterRepository.KEY_ZOOKEEPER_CONNECT);

            Map storage = (Map) data.get("storage");
            List<Map> dataSources = (List) storage.get("datasources");
            dataSources.forEach(m -> {
                Map properties = (Map) m.get("properties");
                String key = ClusterRepository.KEY_BOOTSTRAP_SERVERS;
                properties.put(key, ImmutableMap.of("name", key, "value", bootstrapServers));
                key = ClusterRepository.KEY_BOOTSTRAP_SERVERS + "." + cluster.getId().toLowerCase();
                properties.put(key, ImmutableMap.of("name", key, "value", bootstrapServers));

                String id = (String) m.get("id");
                if (Objects.equals(id, "kafka-consumer")) {
                    key = ClusterRepository.KEY_ZOOKEEPER_CONNECT;
                    properties.put(key, ImmutableMap.of("name", key, "value", zookeeperConnect));
                    key = ClusterRepository.KEY_ZOOKEEPER_CONNECT + "." + cluster.getId().toLowerCase();
                    properties.put(key, ImmutableMap.of("name", key, "value", zookeeperConnect));
                }
            });

            return data;
        } catch (HttpConnectException | HttpTimeoutException e) {
            String message = "kafka hermes proxy invoke api failed: " + url + ", message: " + e.getMessage();
            _logger.error(message, e);
            response.setStatus(500);
            return message;
        } catch (ApacheHttpRequestException e) {
            response.setStatus(e.statusCode());
            return e.responseBody();
        } catch (Exception e) {
            String message = "exception thrown in service execution: " + e.getMessage();
            _logger.error(message, e);
            response.setStatus(500);
            return message;
        }
    }

    protected String getTopicApiUrl() {
        return _hermesPortalApiUrl.getValue() + "/topics/";
    }
}
