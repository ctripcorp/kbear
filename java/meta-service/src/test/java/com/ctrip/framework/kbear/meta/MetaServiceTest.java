package com.ctrip.framework.kbear.meta;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;

import org.junit.runners.Parameterized.Parameters;
import org.springframework.boot.test.context.SpringBootTest;

import com.ctrip.framework.kbear.meta.repository.ClusterRepositoryTest;
import com.ctrip.framework.kbear.meta.repository.ConsumerGroupRepositoryTest;
import com.ctrip.framework.kbear.meta.repository.TopicRepositoryTest;
import com.ctrip.framework.kbear.rest.App;
import com.ctrip.framework.kbear.service.ServiceTest;

@SpringBootTest(classes = { App.class })
public class MetaServiceTest extends ServiceTest {

    @Parameters(name = "{index}: request={0}, response={1}, method={2}")
    public static Collection<Object[]> data() throws NoSuchMethodException, SecurityException {
        List<Object[]> parameterValues = new ArrayList<>();

        addClusterCases(parameterValues);
        addTopicCases(parameterValues);
        addConsumerGroupCases(parameterValues);

        return parameterValues;
    }

    private static void addClusterCases(List<Object[]> parameterValues)
            throws NoSuchMethodException, SecurityException {
        Method method = MetaService.class.getMethod("fetchClusters", FetchClustersRequest.class);

        FetchClustersRequest request = FetchClustersRequest.newBuilder().addClusterIds(ClusterRepositoryTest.CLUSTER_ID)
                .build();
        FetchClustersResponse response = FetchClustersResponse.newBuilder().setStatus(SUCCESS)
                .addClusters(ClusterRepositoryTest.CLUSTER).build();
        parameterValues.add(new Object[] { request, response, method });

        request = FetchClustersRequest.newBuilder().addClusterIds(ClusterRepositoryTest.CLUSTER_ID)
                .addClusterIds(ClusterRepositoryTest.UNKNOWN_CLUSTER_ID).build();
        response = FetchClustersResponse.newBuilder().setStatus(PARTIAL_FAIL).addClusters(ClusterRepositoryTest.CLUSTER)
                .build();
        parameterValues.add(new Object[] { request, response, method });

        request = FetchClustersRequest.newBuilder().build();
        response = FetchClustersResponse.newBuilder().setStatus(SUCCESS)
                .addAllClusters(ClusterRepositoryTest.ALL_CLUSTERS).build();
        parameterValues.add(new Object[] { request, response, method });
    }

    private static void addTopicCases(List<Object[]> parameterValues) throws NoSuchMethodException, SecurityException {
        Method method = MetaService.class.getMethod("fetchTopics", FetchTopicsRequest.class);

        FetchTopicsRequest request = FetchTopicsRequest.newBuilder().addTopicIds(TopicRepositoryTest.TOPIC_ID).build();
        FetchTopicsResponse response = FetchTopicsResponse.newBuilder().setStatus(SUCCESS)
                .addTopics(TopicRepositoryTest.TOPIC).build();
        parameterValues.add(new Object[] { request, response, method });

        request = FetchTopicsRequest.newBuilder().addTopicIds(TopicRepositoryTest.TOPIC_ID)
                .addTopicIds(TopicRepositoryTest.UNKNOWN_TOPIC_ID).build();
        response = FetchTopicsResponse.newBuilder().setStatus(PARTIAL_FAIL).addTopics(TopicRepositoryTest.TOPIC)
                .build();
        parameterValues.add(new Object[] { request, response, method });

        request = FetchTopicsRequest.newBuilder().build();
        response = FetchTopicsResponse.newBuilder().setStatus(SUCCESS).addAllTopics(TopicRepositoryTest.ALL_TOPICS)
                .build();
        parameterValues.add(new Object[] { request, response, method });
    }

    private static void addConsumerGroupCases(List<Object[]> parameterValues)
            throws NoSuchMethodException, SecurityException {
        Method method = MetaService.class.getMethod("fetchConsumerGroups", FetchConsumerGroupsRequest.class);

        FetchConsumerGroupsRequest request = FetchConsumerGroupsRequest.newBuilder()
                .addConsumerGroupIds(ConsumerGroupRepositoryTest.CONSUMER_GROUP_ID).build();
        FetchConsumerGroupsResponse response = FetchConsumerGroupsResponse.newBuilder().setStatus(SUCCESS)
                .addConsumerGroups(ConsumerGroupRepositoryTest.CONSUMER_GROUP).build();
        parameterValues.add(new Object[] { request, response, method });

        request = FetchConsumerGroupsRequest.newBuilder()
                .addConsumerGroupIds(ConsumerGroupRepositoryTest.CONSUMER_GROUP_ID)
                .addConsumerGroupIds(ConsumerGroupRepositoryTest.UNKNOWN_CONSUMER_GROUP_ID).build();
        response = FetchConsumerGroupsResponse.newBuilder().setStatus(PARTIAL_FAIL)
                .addConsumerGroups(ConsumerGroupRepositoryTest.CONSUMER_GROUP).build();
        parameterValues.add(new Object[] { request, response, method });

        request = FetchConsumerGroupsRequest.newBuilder().build();
        response = FetchConsumerGroupsResponse.newBuilder().setStatus(SUCCESS)
                .addAllConsumerGroups(ConsumerGroupRepositoryTest.ALL_CONSUMER_GROUPS).build();
        parameterValues.add(new Object[] { request, response, method });
    }

    @Inject
    private MetaService _metaService;

    @Override
    protected boolean useContextManager() {
        return true;
    }

    @Override
    protected Object getService() {
        return _metaService;
    }

}
