package com.ctrip.framework.kbear.rest.resource;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;

import com.ctrip.framework.kbear.meta.FetchClustersRequest;
import com.ctrip.framework.kbear.meta.FetchClustersResponse;
import com.ctrip.framework.kbear.meta.FetchConsumerGroupsRequest;
import com.ctrip.framework.kbear.meta.FetchConsumerGroupsResponse;
import com.ctrip.framework.kbear.meta.FetchTopicsRequest;
import com.ctrip.framework.kbear.meta.FetchTopicsResponse;
import com.ctrip.framework.kbear.meta.MetaService;

/**
 * @author koqizhao
 *
 * Nov 29, 2018
 */
public class MetaServiceClient implements MetaService, Closeable {

    private static final String FETCH_CLUSTERS_PROCEDURE = "meta/fetchClusters";
    private static final String FETCH_ALL_CLUSTERS_PROCEDURE = "meta/fetchClusters/all";
    private static final String FETCH_TOPICS_PROCEDURE = "meta/fetchTopics";
    private static final String FETCH_ALL_TOPICS_PROCEDURE = "meta/fetchTopics/all";
    private static final String FETCH_CONSUMER_GROUPS_PROCEDURE = "meta/fetchConsumerGroups";
    private static final String FETCH_ALL_CONSUMER_GROUPS_PROCEDURE = "meta/fetchConsumerGroups/all";

    private static final HashMap<String, String> PROCEDURE_REST_PATH_MAP = new HashMap<>();

    static {
        PROCEDURE_REST_PATH_MAP.put(FETCH_CLUSTERS_PROCEDURE, "/meta/clusters");
        PROCEDURE_REST_PATH_MAP.put(FETCH_ALL_CLUSTERS_PROCEDURE, "/meta/clusters/all");
        PROCEDURE_REST_PATH_MAP.put(FETCH_TOPICS_PROCEDURE, "/meta/topics");
        PROCEDURE_REST_PATH_MAP.put(FETCH_ALL_TOPICS_PROCEDURE, "/meta/topics/all");
        PROCEDURE_REST_PATH_MAP.put(FETCH_CONSUMER_GROUPS_PROCEDURE, "/meta/consumer-groups");
        PROCEDURE_REST_PATH_MAP.put(FETCH_ALL_CONSUMER_GROUPS_PROCEDURE, "/meta/consumer-groups/all");
    }

    private TestRpcClient _rpcClient;

    public MetaServiceClient(String serviceUrl) {
        _rpcClient = new TestRpcClient(serviceUrl, PROCEDURE_REST_PATH_MAP);
    }

    @Override
    public FetchClustersResponse fetchClusters(FetchClustersRequest request) {
        boolean emptyRequest = request.getClusterIdsList().isEmpty();
        String procedure = emptyRequest ? FETCH_ALL_CLUSTERS_PROCEDURE : FETCH_CLUSTERS_PROCEDURE;
        return _rpcClient.invoke(procedure, emptyRequest ? null : request, FetchClustersResponse.class);
    }

    @Override
    public FetchTopicsResponse fetchTopics(FetchTopicsRequest request) {
        boolean emptyRequest = request.getTopicIdsList().isEmpty();
        String procedure = emptyRequest ? FETCH_ALL_TOPICS_PROCEDURE : FETCH_TOPICS_PROCEDURE;
        return _rpcClient.invoke(procedure, emptyRequest ? null : request, FetchTopicsResponse.class);
    }

    @Override
    public FetchConsumerGroupsResponse fetchConsumerGroups(FetchConsumerGroupsRequest request) {
        boolean emptyRequest = request.getConsumerGroupIdsList().isEmpty();
        String procedure = emptyRequest ? FETCH_ALL_CONSUMER_GROUPS_PROCEDURE : FETCH_CONSUMER_GROUPS_PROCEDURE;
        return _rpcClient.invoke(procedure, emptyRequest ? null : request, FetchConsumerGroupsResponse.class);
    }

    @Override
    public void close() throws IOException {
        _rpcClient.close();
    }

}
