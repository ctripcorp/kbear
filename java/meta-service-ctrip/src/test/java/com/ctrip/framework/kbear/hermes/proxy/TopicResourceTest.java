package com.ctrip.framework.kbear.hermes.proxy;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mydotey.java.StringExtension;
import org.mydotey.java.collection.CollectionExtension;
import org.mydotey.rpc.client.http.apache.HttpRequestFactory;
import org.mydotey.rpc.client.http.apache.sync.HttpRequestExecutors;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.TestContextManager;

import com.ctrip.framework.foundation.Foundation;
import com.ctrip.framework.kbear.FFFaker;
import com.ctrip.framework.kbear.meta.repository.ClusterRepository;
import com.ctrip.framework.kbear.rest.App;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author koqizhao
 *
 * Dec 19, 2018
 */
@SpringBootTest(classes = { App.class })
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TopicResourceTest {

    static {
        FFFaker.fake();
    }

    private ConfigurableApplicationContext _applicationContext;

    private String _routeNotChangedTopic = "ubt.custom";
    private String _routeChangedTopic = "fx.kafka.demo.hello.run";

    @Inject
    private TopicResource _topicResource;

    private String _originUrl;
    private String _proxyUrl;

    private ObjectMapper _objectMapper = new ObjectMapper();

    @Before
    public void setUp() throws Exception {
        _applicationContext = SpringApplication.run(App.class);
        new TestContextManager(getClass()).prepareTestInstance(this);

        _originUrl = _topicResource.getTopicApiUrl();
        _proxyUrl = "http://localhost:8081/hermes/topics/";
    }

    @After
    public void tearDown() throws IOException {
        _applicationContext.close();
    }

    @Test
    public void fetchTopic() throws IOException {
        if (Foundation.server().getEnv().isPRO()) {
            System.out.println("not supported in pro, skip");
            return;
        }

        fetchTopic(_routeNotChangedTopic, false);
    }

    @Test
    public void fetchTopic2() throws IOException {
        fetchTopic(_routeChangedTopic, true);
    }

    protected void fetchTopic(String topic, boolean routeChanged) throws IOException {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            String url = _originUrl + topic;
            Map data = fetchTopicData(client, url);
            System.out.printf("original: %s\n\n", data);

            url = _proxyUrl + topic;
            Map data2 = fetchTopicData(client, url);
            System.out.printf("proxy: %s\n\n", data2);

            Assert.assertNotEquals(data, data2);

            List<String> originBootStrapServers = getBootstrapServers(data);
            List<String> proxyBootStrapServers = getBootstrapServers(data2);
            System.out.printf("bootstrap servers, origin: %s, proxy: %s\n\n", originBootStrapServers,
                    proxyBootStrapServers);
            Assert.assertFalse(CollectionExtension.isEmpty(originBootStrapServers));
            Assert.assertEquals(2, originBootStrapServers.size());
            originBootStrapServers.forEach(s -> Assert.assertFalse(StringExtension.isBlank(s)));

            String originZookeeper = getZKConnect(data);
            String proxyZookeeper = getZKConnect(data2);
            System.out.printf("zookeeper connect, origin: %s, proxy: %s\n\n", originZookeeper, proxyZookeeper);
            Assert.assertFalse(StringExtension.isBlank(originZookeeper));

            if (routeChanged) {
                Assert.assertNotEquals(originBootStrapServers, proxyBootStrapServers);
                Assert.assertNotEquals(originZookeeper, proxyZookeeper);
            } else {
                Assert.assertEquals(originBootStrapServers, proxyBootStrapServers);
                Assert.assertEquals(originZookeeper, proxyZookeeper);
            }
        }
    }

    protected Map fetchTopicData(CloseableHttpClient client, String url) {
        HttpUriRequest request = HttpRequestFactory.createRequest(url, HttpGet.METHOD_NAME);
        return HttpRequestExecutors.execute(client, request, r -> {
            try {
                return _objectMapper.readValue(r.getEntity().getContent(), Map.class);
            } catch (UnsupportedOperationException | IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected List<String> getBootstrapServers(Map data) {
        Map storage = (Map) data.get("storage");
        List<Map> dataSources = (List) storage.get("datasources");
        String[] results = new String[2];
        dataSources.forEach(m -> {
            Map properties = (Map) m.get("properties");
            String key = ClusterRepository.KEY_BOOTSTRAP_SERVERS;
            Map property = (Map) properties.get(key);
            String value = (String) property.get("value");
            String id = (String) m.get("id");
            switch (id) {
                case "kafka-producer":
                    results[0] = value;
                    break;
                case "kafka-consumer":
                    results[1] = value;
                    break;
                default:
                    break;
            }
        });

        return Arrays.asList(results);
    }

    protected String getZKConnect(Map data) {
        Map storage = (Map) data.get("storage");
        List<Map> dataSources = (List) storage.get("datasources");
        AtomicReference<String> result = new AtomicReference<String>();
        dataSources.forEach(m -> {
            Map properties = (Map) m.get("properties");
            String id = (String) m.get("id");
            if (!Objects.equals(id, "kafka-consumer"))
                return;

            String key = ClusterRepository.KEY_ZOOKEEPER_CONNECT;
            Map property = (Map) properties.get(key);
            String value = (String) property.get("value");
            result.set(value);
        });

        return result.get();
    }

}
