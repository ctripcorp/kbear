package com.ctrip.framework.kbear.meta;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.mydotey.java.ObjectExtension;
import org.mydotey.rpc.client.http.HttpServiceClient;
import org.mydotey.rpc.client.http.HttpServiceClientConfig;

/**
 * @author koqizhao
 *
 * Dec 18, 2018
 */
public class MetaServiceClient implements MetaService, AutoCloseable {

    private static final String FETCH_CLUSTERS_PROCEDURE = "meta/fetchClusters";
    private static final String FETCH_TOPICS_PROCEDURE = "meta/fetchTopics";
    private static final String FETCH_CONSUMER_GROUPS_PROCEDURE = "meta/fetchConsumerGroups";

    public static final Map<String, String> PROCEDURE_REST_PATH_MAP;

    static {
        HashMap<String, String> map = new HashMap<>();
        map.put(FETCH_CLUSTERS_PROCEDURE, "/meta/clusters");
        map.put(FETCH_TOPICS_PROCEDURE, "/meta/topics");
        map.put(FETCH_CONSUMER_GROUPS_PROCEDURE, "/meta/consumer-groups");
        PROCEDURE_REST_PATH_MAP = Collections.unmodifiableMap(map);
    }

    private HttpServiceClient _serviceClient;

    public MetaServiceClient(HttpServiceClientConfig config) {
        ObjectExtension.requireNonNull(config, "config");
        _serviceClient = new HttpServiceClient(config);
    }

    public HttpServiceClientConfig getConfig() {
        return _serviceClient.getConfig();
    }

    @Override
    public FetchClustersResponse fetchClusters(FetchClustersRequest request) {
        return _serviceClient.invoke(FETCH_CLUSTERS_PROCEDURE, request, FetchClustersResponse.class);
    }

    @Override
    public FetchTopicsResponse fetchTopics(FetchTopicsRequest request) {
        return _serviceClient.invoke(FETCH_TOPICS_PROCEDURE, request, FetchTopicsResponse.class);
    }

    @Override
    public FetchConsumerGroupsResponse fetchConsumerGroups(FetchConsumerGroupsRequest request) {
        return _serviceClient.invoke(FETCH_CONSUMER_GROUPS_PROCEDURE, request, FetchConsumerGroupsResponse.class);
    }

    @Override
    public void close() throws Exception {
        _serviceClient.close();
    }

}
