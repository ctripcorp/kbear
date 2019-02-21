package com.ctrip.framework.kbear.route;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;

import org.junit.runners.Parameterized.Parameters;
import org.springframework.boot.test.context.SpringBootTest;

import com.ctrip.framework.kbear.meta.ConsumerGroup;
import com.ctrip.framework.kbear.meta.ConsumerGroupId;
import com.ctrip.framework.kbear.meta.repository.ClusterRepositoryTest;
import com.ctrip.framework.kbear.meta.repository.ConsumerGroupRepositoryTest;
import com.ctrip.framework.kbear.meta.repository.TopicRepositoryTest;
import com.ctrip.framework.kbear.rest.App;
import com.ctrip.framework.kbear.route.DefaultRouteService.RouteRules;
import com.ctrip.framework.kbear.route.repository.RouteRuleRepositoryTest;
import com.ctrip.framework.kbear.service.ServiceTest;

@SpringBootTest(classes = { App.class })
public class RouteServiceTest extends ServiceTest {

    public static final Client EMPTY_CLIENT = Client.newBuilder().build();
    public static final Client CLIENT = Client.newBuilder().setId("100000001").build();
    public static final Client CLIENT_2 = Client.newBuilder().putMeta("env", "UAT").build();

    @Parameters(name = "{index}: request={0}, response={1}, method={2}")
    public static Collection<Object[]> data() throws NoSuchMethodException, SecurityException {
        List<Object[]> parameterValues = new ArrayList<>();

        addRouteRuleCases(parameterValues);
        addProducerRouteCases(parameterValues);
        addConsumerRouteCases(parameterValues);

        return parameterValues;
    }

    private static void addRouteRuleCases(List<Object[]> parameterValues)
            throws NoSuchMethodException, SecurityException {
        RouteRuleInfo routeRuleInfo = RouteRules.getInfo(RouteRuleRepositoryTest.ROUTE_RULE);
        List<RouteRuleInfo> allInfos = new ArrayList<>();
        RouteRuleRepositoryTest.ALL_ROUTE_RULES.forEach(r -> allInfos.add(RouteRules.getInfo(r)));

        Method method = RouteService.class.getMethod("fetchRouteRules", FetchRouteRulesRequest.class);

        FetchRouteRulesRequest request = FetchRouteRulesRequest.newBuilder()
                .addRouteRuleIds(RouteRuleRepositoryTest.ROUTE_RULE_ID).build();
        FetchRouteRulesResponse response = FetchRouteRulesResponse.newBuilder().setStatus(SUCCESS)
                .addRouteRules(routeRuleInfo).build();
        parameterValues.add(new Object[] { request, response, method });

        request = FetchRouteRulesRequest.newBuilder().addRouteRuleIds(RouteRuleRepositoryTest.ROUTE_RULE_ID)
                .addRouteRuleIds(RouteRuleRepositoryTest.UNKNOWN_ROUTE_RULE_ID).build();
        response = FetchRouteRulesResponse.newBuilder().setStatus(PARTIAL_FAIL).addRouteRules(routeRuleInfo).build();
        parameterValues.add(new Object[] { request, response, method });

        request = FetchRouteRulesRequest.newBuilder().build();
        response = FetchRouteRulesResponse.newBuilder().setStatus(SUCCESS).addAllRouteRules(allInfos).build();
        parameterValues.add(new Object[] { request, response, method });
    }

    private static void addProducerRouteCases(List<Object[]> parameterValues)
            throws NoSuchMethodException, SecurityException {
        Method method = RouteService.class.getMethod("fetchProducerRoute", FetchProducerRouteRequest.class);

        FetchProducerRouteRequest request = FetchProducerRouteRequest.newBuilder().setClient(EMPTY_CLIENT)
                .addTopicIds(TopicRepositoryTest.TOPIC_ID_2).build();
        FetchProducerRouteResponse response = FetchProducerRouteResponse.newBuilder().setStatus(SUCCESS)
                .putTopicIdRoutes(TopicRepositoryTest.TOPIC_ID_2,
                        Route.newBuilder().setClusterId(ClusterRepositoryTest.CLUSTER_ID)
                                .setTopicId(TopicRepositoryTest.TOPIC_ID_2).build())
                .putClusters(ClusterRepositoryTest.CLUSTER_ID, ClusterRepositoryTest.CLUSTER)
                .putTopics(TopicRepositoryTest.TOPIC_ID_2, TopicRepositoryTest.TOPIC_2).build();
        parameterValues.add(new Object[] { request, response, method });

        request = FetchProducerRouteRequest.newBuilder().setClient(EMPTY_CLIENT)
                .addTopicIds(TopicRepositoryTest.TOPIC_ID).build();
        response = FetchProducerRouteResponse.newBuilder().setStatus(SUCCESS)
                .putTopicIdRoutes(TopicRepositoryTest.TOPIC_ID,
                        Route.newBuilder().setClusterId(ClusterRepositoryTest.CLUSTER_ID_2)
                                .setTopicId(TopicRepositoryTest.TOPIC_ID).build())
                .putClusters(ClusterRepositoryTest.CLUSTER_ID_2, ClusterRepositoryTest.CLUSTER_2)
                .putTopics(TopicRepositoryTest.TOPIC_ID, TopicRepositoryTest.TOPIC).build();
        parameterValues.add(new Object[] { request, response, method });

        request = FetchProducerRouteRequest.newBuilder().setClient(EMPTY_CLIENT)
                .addTopicIds(TopicRepositoryTest.UNKNOWN_TOPIC_ID).build();
        response = FetchProducerRouteResponse.newBuilder().setStatus(SUCCESS)
                .putTopicIdRoutes(TopicRepositoryTest.UNKNOWN_TOPIC_ID,
                        Route.newBuilder().setClusterId(ClusterRepositoryTest.CLUSTER_ID)
                                .setTopicId(TopicRepositoryTest.UNKNOWN_TOPIC_ID).build())
                .putClusters(ClusterRepositoryTest.CLUSTER_ID, ClusterRepositoryTest.CLUSTER)
                .putTopics(TopicRepositoryTest.UNKNOWN_TOPIC_ID, TopicRepositoryTest.UNKNOWN_TOPIC).build();
        parameterValues.add(new Object[] { request, response, method });

        request = FetchProducerRouteRequest.newBuilder().setClient(CLIENT).addTopicIds(TopicRepositoryTest.TOPIC_ID)
                .build();
        response = FetchProducerRouteResponse.newBuilder().setStatus(SUCCESS)
                .putTopicIdRoutes(TopicRepositoryTest.TOPIC_ID,
                        Route.newBuilder().setClusterId(ClusterRepositoryTest.CLUSTER_ID_3)
                                .setTopicId(TopicRepositoryTest.TOPIC_ID).build())
                .putClusters(ClusterRepositoryTest.CLUSTER_ID_3, ClusterRepositoryTest.CLUSTER_3)
                .putTopics(TopicRepositoryTest.TOPIC_ID, TopicRepositoryTest.TOPIC).build();
        parameterValues.add(new Object[] { request, response, method });

        request = FetchProducerRouteRequest.newBuilder().setClient(CLIENT_2).addTopicIds(TopicRepositoryTest.TOPIC_ID)
                .build();
        response = FetchProducerRouteResponse.newBuilder().setStatus(SUCCESS)
                .putTopicIdRoutes(TopicRepositoryTest.TOPIC_ID,
                        Route.newBuilder().setClusterId(ClusterRepositoryTest.CLUSTER_ID_2)
                                .setTopicId(TopicRepositoryTest.TOPIC_ID).build())
                .putClusters(ClusterRepositoryTest.CLUSTER_ID_2, ClusterRepositoryTest.CLUSTER_2)
                .putTopics(TopicRepositoryTest.TOPIC_ID, TopicRepositoryTest.TOPIC).build();
        parameterValues.add(new Object[] { request, response, method });

        request = FetchProducerRouteRequest.newBuilder().setClient(CLIENT_2).addTopicIds(TopicRepositoryTest.TOPIC_ID_2)
                .build();
        response = FetchProducerRouteResponse.newBuilder().setStatus(SUCCESS)
                .putTopicIdRoutes(TopicRepositoryTest.TOPIC_ID_2,
                        Route.newBuilder().setClusterId(ClusterRepositoryTest.CLUSTER_ID_2)
                                .setTopicId(TopicRepositoryTest.TOPIC_ID_2).build())
                .putClusters(ClusterRepositoryTest.CLUSTER_ID_2, ClusterRepositoryTest.CLUSTER_2)
                .putTopics(TopicRepositoryTest.TOPIC_ID_2, TopicRepositoryTest.TOPIC_2).build();
        parameterValues.add(new Object[] { request, response, method });

        request = FetchProducerRouteRequest.newBuilder().setClient(CLIENT).addTopicIds(TopicRepositoryTest.TOPIC_ID)
                .addTopicIds(TopicRepositoryTest.TOPIC_ID_2).addTopicIds(TopicRepositoryTest.UNKNOWN_TOPIC_ID).build();
        response = FetchProducerRouteResponse.newBuilder().setStatus(SUCCESS)
                .putTopicIdRoutes(TopicRepositoryTest.TOPIC_ID,
                        Route.newBuilder().setClusterId(ClusterRepositoryTest.CLUSTER_ID_3)
                                .setTopicId(TopicRepositoryTest.TOPIC_ID).build())
                .putTopicIdRoutes(TopicRepositoryTest.TOPIC_ID_2,
                        Route.newBuilder().setClusterId(ClusterRepositoryTest.CLUSTER_ID)
                                .setTopicId(TopicRepositoryTest.TOPIC_ID_2).build())
                .putTopicIdRoutes(TopicRepositoryTest.UNKNOWN_TOPIC_ID,
                        Route.newBuilder().setClusterId(ClusterRepositoryTest.CLUSTER_ID)
                                .setTopicId(TopicRepositoryTest.UNKNOWN_TOPIC_ID).build())
                .putClusters(ClusterRepositoryTest.CLUSTER_ID, ClusterRepositoryTest.CLUSTER)
                .putClusters(ClusterRepositoryTest.CLUSTER_ID_3, ClusterRepositoryTest.CLUSTER_3)
                .putTopics(TopicRepositoryTest.TOPIC_ID, TopicRepositoryTest.TOPIC)
                .putTopics(TopicRepositoryTest.TOPIC_ID_2, TopicRepositoryTest.TOPIC_2)
                .putTopics(TopicRepositoryTest.UNKNOWN_TOPIC_ID, TopicRepositoryTest.UNKNOWN_TOPIC).build();
        parameterValues.add(new Object[] { request, response, method });
    }

    private static void addConsumerRouteCases(List<Object[]> parameterValues)
            throws NoSuchMethodException, SecurityException {
        Method method = RouteService.class.getMethod("fetchConsumerRoute", FetchConsumerRouteRequest.class);

        ConsumerGroupId consumerGroupId = ConsumerGroupRepositoryTest.UNKNOWN_CONSUMER_GROUP_ID;
        FetchConsumerRouteRequest request = FetchConsumerRouteRequest.newBuilder().setClient(EMPTY_CLIENT)
                .addConsumerGroupIds(consumerGroupId).build();
        FetchConsumerRouteResponse response = FetchConsumerRouteResponse.newBuilder().setStatus(SUCCESS)
                .addConsumerGroupIdRoutes(ConsumerGroupIdRoutePair.newBuilder().setConsumerGroupId(consumerGroupId)
                        .setRoute(Route.newBuilder().setClusterId(ClusterRepositoryTest.CLUSTER_ID)
                                .setTopicId(consumerGroupId.getTopicId()).build())
                        .build())
                .putClusters(ClusterRepositoryTest.CLUSTER_ID, ClusterRepositoryTest.CLUSTER)
                .putTopics(TopicRepositoryTest.UNKNOWN_TOPIC_ID, TopicRepositoryTest.UNKNOWN_TOPIC)
                .addConsumerGroups(ConsumerGroupRepositoryTest.UNKNOWN_CONSUMER_GROUP).build();
        parameterValues.add(new Object[] { request, response, method });

        consumerGroupId = ConsumerGroupId.newBuilder()
                .setGroupName(ConsumerGroupRepositoryTest.UNKNOWN_CONSUMER_GROUP_NAME)
                .setTopicId(TopicRepositoryTest.TOPIC_ID).build();
        request = FetchConsumerRouteRequest.newBuilder().setClient(EMPTY_CLIENT).addConsumerGroupIds(consumerGroupId)
                .build();
        response = FetchConsumerRouteResponse.newBuilder().setStatus(SUCCESS)
                .addConsumerGroupIdRoutes(ConsumerGroupIdRoutePair.newBuilder().setConsumerGroupId(consumerGroupId)
                        .setRoute(Route.newBuilder().setClusterId(ClusterRepositoryTest.CLUSTER_ID_2)
                                .setTopicId(consumerGroupId.getTopicId()).build())
                        .build())
                .putClusters(ClusterRepositoryTest.CLUSTER_ID_2, ClusterRepositoryTest.CLUSTER_2)
                .putTopics(TopicRepositoryTest.TOPIC_ID, TopicRepositoryTest.TOPIC)
                .addConsumerGroups(ConsumerGroup.newBuilder().setId(consumerGroupId).build()).build();
        parameterValues.add(new Object[] { request, response, method });

        consumerGroupId = ConsumerGroupId.newBuilder()
                .setGroupName(ConsumerGroupRepositoryTest.UNKNOWN_CONSUMER_GROUP_NAME)
                .setTopicId(TopicRepositoryTest.TOPIC_ID).build();
        request = FetchConsumerRouteRequest.newBuilder().setClient(CLIENT).addConsumerGroupIds(consumerGroupId).build();
        response = FetchConsumerRouteResponse.newBuilder().setStatus(SUCCESS)
                .addConsumerGroupIdRoutes(ConsumerGroupIdRoutePair.newBuilder().setConsumerGroupId(consumerGroupId)
                        .setRoute(Route.newBuilder().setClusterId(ClusterRepositoryTest.CLUSTER_ID_3)
                                .setTopicId(consumerGroupId.getTopicId()).build())
                        .build())
                .putClusters(ClusterRepositoryTest.CLUSTER_ID_3, ClusterRepositoryTest.CLUSTER_3)
                .putTopics(TopicRepositoryTest.TOPIC_ID, TopicRepositoryTest.TOPIC)
                .addConsumerGroups(ConsumerGroup.newBuilder().setId(consumerGroupId).build()).build();
        parameterValues.add(new Object[] { request, response, method });

        consumerGroupId = ConsumerGroupRepositoryTest.CONSUMER_GROUP_ID;
        request = FetchConsumerRouteRequest.newBuilder().setClient(EMPTY_CLIENT).addConsumerGroupIds(consumerGroupId)
                .build();
        response = FetchConsumerRouteResponse.newBuilder().setStatus(SUCCESS)
                .addConsumerGroupIdRoutes(ConsumerGroupIdRoutePair.newBuilder().setConsumerGroupId(consumerGroupId)
                        .setRoute(Route.newBuilder().setClusterId(ClusterRepositoryTest.CLUSTER_ID_3)
                                .setTopicId(consumerGroupId.getTopicId()).build())
                        .build())
                .putClusters(ClusterRepositoryTest.CLUSTER_ID_3, ClusterRepositoryTest.CLUSTER_3)
                .putTopics(TopicRepositoryTest.TOPIC_ID, TopicRepositoryTest.TOPIC)
                .addConsumerGroups(ConsumerGroupRepositoryTest.CONSUMER_GROUP).build();
        parameterValues.add(new Object[] { request, response, method });

        consumerGroupId = ConsumerGroupRepositoryTest.CONSUMER_GROUP_ID_3;
        request = FetchConsumerRouteRequest.newBuilder().setClient(EMPTY_CLIENT).addConsumerGroupIds(consumerGroupId)
                .build();
        response = FetchConsumerRouteResponse.newBuilder().setStatus(SUCCESS)
                .addConsumerGroupIdRoutes(ConsumerGroupIdRoutePair.newBuilder().setConsumerGroupId(consumerGroupId)
                        .setRoute(Route.newBuilder().setClusterId(ClusterRepositoryTest.CLUSTER_ID)
                                .setTopicId(TopicRepositoryTest.TOPIC_ID_2).build())
                        .build())
                .putClusters(ClusterRepositoryTest.CLUSTER_ID, ClusterRepositoryTest.CLUSTER)
                .putTopics(TopicRepositoryTest.TOPIC_ID_2, TopicRepositoryTest.TOPIC_2)
                .addConsumerGroups(ConsumerGroupRepositoryTest.CONSUMER_GROUP_3).build();
        parameterValues.add(new Object[] { request, response, method });

        consumerGroupId = ConsumerGroupRepositoryTest.CONSUMER_GROUP_ID;
        request = FetchConsumerRouteRequest.newBuilder().setClient(CLIENT_2).addConsumerGroupIds(consumerGroupId)
                .build();
        response = FetchConsumerRouteResponse.newBuilder().setStatus(SUCCESS)
                .addConsumerGroupIdRoutes(ConsumerGroupIdRoutePair.newBuilder().setConsumerGroupId(consumerGroupId)
                        .setRoute(Route.newBuilder().setClusterId(ClusterRepositoryTest.CLUSTER_ID_2)
                                .setTopicId(consumerGroupId.getTopicId()).build())
                        .build())
                .putClusters(ClusterRepositoryTest.CLUSTER_ID_2, ClusterRepositoryTest.CLUSTER_2)
                .putTopics(TopicRepositoryTest.TOPIC_ID, TopicRepositoryTest.TOPIC)
                .addConsumerGroups(ConsumerGroupRepositoryTest.CONSUMER_GROUP).build();
        parameterValues.add(new Object[] { request, response, method });

        consumerGroupId = ConsumerGroupRepositoryTest.CONSUMER_GROUP_ID_2;
        request = FetchConsumerRouteRequest.newBuilder().setClient(EMPTY_CLIENT).addConsumerGroupIds(consumerGroupId)
                .build();
        response = FetchConsumerRouteResponse.newBuilder().setStatus(SUCCESS)
                .addConsumerGroupIdRoutes(ConsumerGroupIdRoutePair.newBuilder().setConsumerGroupId(consumerGroupId)
                        .setRoute(Route.newBuilder().setClusterId(ClusterRepositoryTest.CLUSTER_ID)
                                .setTopicId(consumerGroupId.getTopicId()).build())
                        .build())
                .putClusters(ClusterRepositoryTest.CLUSTER_ID, ClusterRepositoryTest.CLUSTER)
                .putTopics(TopicRepositoryTest.TOPIC_ID_2, TopicRepositoryTest.TOPIC_2)
                .addConsumerGroups(ConsumerGroupRepositoryTest.CONSUMER_GROUP_2).build();
        parameterValues.add(new Object[] { request, response, method });

        request = FetchConsumerRouteRequest.newBuilder().setClient(EMPTY_CLIENT)
                .addConsumerGroupIds(ConsumerGroupRepositoryTest.CONSUMER_GROUP_ID)
                .addConsumerGroupIds(ConsumerGroupRepositoryTest.CONSUMER_GROUP_ID_3).build();
        response = FetchConsumerRouteResponse.newBuilder().setStatus(SUCCESS)
                .addConsumerGroupIdRoutes(ConsumerGroupIdRoutePair.newBuilder()
                        .setConsumerGroupId(ConsumerGroupRepositoryTest.CONSUMER_GROUP_ID)
                        .setRoute(Route.newBuilder().setClusterId(ClusterRepositoryTest.CLUSTER_ID_3)
                                .setTopicId(ConsumerGroupRepositoryTest.CONSUMER_GROUP_ID.getTopicId()).build())
                        .build())
                .addConsumerGroupIdRoutes(ConsumerGroupIdRoutePair.newBuilder()
                        .setConsumerGroupId(ConsumerGroupRepositoryTest.CONSUMER_GROUP_ID_3)
                        .setRoute(Route.newBuilder().setClusterId(ClusterRepositoryTest.CLUSTER_ID)
                                .setTopicId(TopicRepositoryTest.TOPIC_ID_2).build())
                        .build())
                .putClusters(ClusterRepositoryTest.CLUSTER_ID, ClusterRepositoryTest.CLUSTER)
                .putClusters(ClusterRepositoryTest.CLUSTER_ID_3, ClusterRepositoryTest.CLUSTER_3)
                .putTopics(TopicRepositoryTest.TOPIC_ID, TopicRepositoryTest.TOPIC)
                .putTopics(TopicRepositoryTest.TOPIC_ID_2, TopicRepositoryTest.TOPIC_2)
                .addConsumerGroups(ConsumerGroupRepositoryTest.CONSUMER_GROUP)
                .addConsumerGroups(ConsumerGroupRepositoryTest.CONSUMER_GROUP_3).build();
        parameterValues.add(new Object[] { request, response, method });

        request = FetchConsumerRouteRequest.newBuilder().setClient(EMPTY_CLIENT)
                .addConsumerGroupIds(ConsumerGroupRepositoryTest.CONSUMER_GROUP_ID)
                .addConsumerGroupIds(ConsumerGroupRepositoryTest.CONSUMER_GROUP_ID_2)
                .addConsumerGroupIds(ConsumerGroupRepositoryTest.CONSUMER_GROUP_ID_3)
                .addConsumerGroupIds(ConsumerGroupRepositoryTest.UNKNOWN_CONSUMER_GROUP_ID).build();
        response = FetchConsumerRouteResponse.newBuilder().setStatus(SUCCESS)
                .addConsumerGroupIdRoutes(ConsumerGroupIdRoutePair.newBuilder()
                        .setConsumerGroupId(ConsumerGroupRepositoryTest.CONSUMER_GROUP_ID)
                        .setRoute(Route.newBuilder().setClusterId(ClusterRepositoryTest.CLUSTER_ID_3)
                                .setTopicId(ConsumerGroupRepositoryTest.CONSUMER_GROUP_ID.getTopicId()).build())
                        .build())
                .addConsumerGroupIdRoutes(ConsumerGroupIdRoutePair.newBuilder()
                        .setConsumerGroupId(ConsumerGroupRepositoryTest.CONSUMER_GROUP_ID_2)
                        .setRoute(Route.newBuilder().setClusterId(ClusterRepositoryTest.CLUSTER_ID)
                                .setTopicId(ConsumerGroupRepositoryTest.CONSUMER_GROUP_ID_2.getTopicId()).build())
                        .build())
                .addConsumerGroupIdRoutes(ConsumerGroupIdRoutePair.newBuilder()
                        .setConsumerGroupId(ConsumerGroupRepositoryTest.CONSUMER_GROUP_ID_3)
                        .setRoute(Route.newBuilder().setClusterId(ClusterRepositoryTest.CLUSTER_ID)
                                .setTopicId(TopicRepositoryTest.TOPIC_ID_2).build())
                        .build())
                .addConsumerGroupIdRoutes(ConsumerGroupIdRoutePair.newBuilder()
                        .setConsumerGroupId(ConsumerGroupRepositoryTest.UNKNOWN_CONSUMER_GROUP_ID)
                        .setRoute(Route.newBuilder().setClusterId(ClusterRepositoryTest.CLUSTER_ID)
                                .setTopicId(TopicRepositoryTest.UNKNOWN_TOPIC_ID).build())
                        .build())
                .putClusters(ClusterRepositoryTest.CLUSTER_ID, ClusterRepositoryTest.CLUSTER)
                .putClusters(ClusterRepositoryTest.CLUSTER_ID_3, ClusterRepositoryTest.CLUSTER_3)
                .putTopics(TopicRepositoryTest.TOPIC_ID, TopicRepositoryTest.TOPIC)
                .putTopics(TopicRepositoryTest.TOPIC_ID_2, TopicRepositoryTest.TOPIC_2)
                .putTopics(TopicRepositoryTest.UNKNOWN_TOPIC_ID, TopicRepositoryTest.UNKNOWN_TOPIC)
                .addConsumerGroups(ConsumerGroupRepositoryTest.CONSUMER_GROUP)
                .addConsumerGroups(ConsumerGroupRepositoryTest.CONSUMER_GROUP_2)
                .addConsumerGroups(ConsumerGroupRepositoryTest.CONSUMER_GROUP_3)
                .addConsumerGroups(ConsumerGroupRepositoryTest.UNKNOWN_CONSUMER_GROUP).build();
        parameterValues.add(new Object[] { request, response, method });
    }

    @Inject
    private RouteService _routeService;

    @Override
    protected boolean useContextManager() {
        return true;
    }

    @Override
    protected Object getService() {
        return _routeService;
    }

}
