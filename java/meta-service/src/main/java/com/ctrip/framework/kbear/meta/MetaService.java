package com.ctrip.framework.kbear.meta;

public interface MetaService {

    FetchClustersResponse fetchClusters(FetchClustersRequest request);

    FetchTopicsResponse fetchTopics(FetchTopicsRequest request);

    FetchConsumerGroupsResponse fetchConsumerGroups(FetchConsumerGroupsRequest request);

}
