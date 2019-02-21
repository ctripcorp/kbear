package com.ctrip.framework.kbear.route;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.mydotey.java.StringExtension;
import org.mydotey.rpc.ack.Acks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.framework.kbear.Util;
import com.ctrip.framework.kbear.meta.Cluster;
import com.ctrip.framework.kbear.meta.ConsumerGroup;
import com.ctrip.framework.kbear.meta.ConsumerGroupId;
import com.ctrip.framework.kbear.meta.Topic;
import com.ctrip.framework.kbear.meta.repository.ClusterRepository;
import com.ctrip.framework.kbear.meta.repository.ConsumerGroupRepository;
import com.ctrip.framework.kbear.meta.repository.TopicRepository;
import com.ctrip.framework.kbear.route.RouteRule;
import com.ctrip.framework.kbear.route.DefaultRouteService.RouteRules.MetaKeys;
import com.ctrip.framework.kbear.route.crrl.CrrlRouteRule;
import com.ctrip.framework.kbear.route.repository.RouteRuleRepository;
import com.ctrip.framework.kbear.service.ResponseStatus;

/**
 * @author koqizhao
 *
 * Sep 21, 2018
 */
@Singleton
@Named()
public class DefaultRouteService implements RouteService {

    private static Logger _logger = LoggerFactory.getLogger(DefaultRouteService.class);

    @Inject
    private ClusterRepository _clusterRepository;

    @Inject
    private TopicRepository _topicRepository;

    @Inject
    private ConsumerGroupRepository _consumerGroupRepository;

    @Inject
    private RouteRuleRepository _routeRuleRepository;

    @Override
    public FetchRouteRulesResponse fetchRouteRules(FetchRouteRulesRequest request) {
        FetchRouteRulesResponse.Builder builder = FetchRouteRulesResponse.newBuilder();
        ResponseStatus.Builder statusBuilder = ResponseStatus.newBuilder();
        try {
            boolean isEmpty = request == null || request.getRouteRuleIdsList().isEmpty();
            List<RouteRule> routeRules;
            if (isEmpty) {
                routeRules = _routeRuleRepository.getAll();
                statusBuilder.setAck(Acks.SUCCESS);
            } else {
                routeRules = _routeRuleRepository.getRecords(request.getRouteRuleIdsList());
                statusBuilder.setAck(
                        routeRules.size() == request.getRouteRuleIdsList().size() ? Acks.SUCCESS : Acks.PARTIAL_FAIL);
            }
            routeRules.forEach(r -> builder.addRouteRules(RouteRules.getInfo(r)));
            return builder.setStatus(statusBuilder.build()).build();
        } catch (Exception e) {
            Util.handleServiceException(statusBuilder, e, _logger);
            return builder.setStatus(statusBuilder.build()).build();
        }
    }

    @Override
    public FetchProducerRouteResponse fetchProducerRoute(FetchProducerRouteRequest request) {
        FetchProducerRouteResponse.Builder builder = FetchProducerRouteResponse.newBuilder();
        ResponseStatus.Builder statusBuilder = ResponseStatus.newBuilder();
        if (request == null || request.getTopicIdsList().isEmpty()) {
            Util.handleBadReqeust(statusBuilder, request);
            return builder.setStatus(statusBuilder.build()).build();
        }

        try {
            request.getTopicIdsList().forEach(id -> {
                Topic topic = toTopic(id);
                Map<String, String> routeFactors = new HashMap<>();
                addRouteFactors(routeFactors, request.getClient());
                addRouteFactors(routeFactors, topic);

                List<RouteRule> routeRules = _routeRuleRepository.getRecords(id);
                Route route = applyRouteRule(routeRules, routeFactors, id);
                if (route == null)
                    return;

                builder.putTopicIdRoutes(route.getTopicId(), route);
                builder.putClusters(route.getClusterId(), _clusterRepository.getRecord(route.getClusterId()));
                builder.putTopics(route.getTopicId(), toTopic(route.getTopicId()));
            });

            statusBuilder.setAck(builder.getTopicIdRoutesMap().size() == request.getTopicIdsList().size() ? Acks.SUCCESS
                    : Acks.PARTIAL_FAIL);
            return builder.setStatus(statusBuilder.build()).build();
        } catch (Exception e) {
            Util.handleServiceException(statusBuilder, e, _logger);
            return builder.setStatus(statusBuilder.build()).build();
        }
    }

    @Override
    public FetchConsumerRouteResponse fetchConsumerRoute(FetchConsumerRouteRequest request) {
        FetchConsumerRouteResponse.Builder builder = FetchConsumerRouteResponse.newBuilder();
        ResponseStatus.Builder statusBuilder = ResponseStatus.newBuilder();
        if (request == null || request.getConsumerGroupIdsList().isEmpty()) {
            Util.handleBadReqeust(statusBuilder, request);
            return builder.setStatus(statusBuilder.build()).build();
        }

        try {
            request.getConsumerGroupIdsList().forEach(id -> {
                Topic topic = toTopic(id.getTopicId());
                ConsumerGroup consumerGroup = toConsumerGroup(id);

                Map<String, String> routeFactors = new HashMap<>();
                addRouteFactors(routeFactors, request.getClient());
                addRouteFactors(routeFactors, topic);
                addRouteFactors(routeFactors, consumerGroup);

                List<RouteRule> routeRules = _routeRuleRepository.getRecords(id);
                Route route = applyRouteRule(routeRules, routeFactors, topic.getId());
                if (route == null)
                    return;

                builder.addConsumerGroupIdRoutes(
                        ConsumerGroupIdRoutePair.newBuilder().setConsumerGroupId(id).setRoute(route).build());
                builder.putClusters(route.getClusterId(), _clusterRepository.getRecord(route.getClusterId()));
                builder.putTopics(route.getTopicId(), toTopic(route.getTopicId()));
                builder.addConsumerGroups(consumerGroup);
            });

            statusBuilder
                    .setAck(builder.getConsumerGroupIdRoutesList().size() == request.getConsumerGroupIdsList().size()
                            ? Acks.SUCCESS
                            : Acks.PARTIAL_FAIL);
            return builder.setStatus(statusBuilder.build()).build();
        } catch (Exception e) {
            Util.handleServiceException(statusBuilder, e, _logger);
            return builder.setStatus(statusBuilder.build()).build();
        }
    }

    private Topic toTopic(String topicId) {
        Topic topic = _topicRepository.getRecord(topicId);
        if (topic == null)
            topic = Topic.newBuilder().setId(topicId).build();
        return topic;
    }

    private ConsumerGroup toConsumerGroup(ConsumerGroupId consumerGroupId) {
        ConsumerGroup consumerGroup = _consumerGroupRepository.getRecord(consumerGroupId);
        if (consumerGroup == null)
            consumerGroup = ConsumerGroup.newBuilder().setId(consumerGroupId).build();
        return consumerGroup;
    }

    private Map<String, String> addRouteFactors(Map<String, String> routeFactors, Client client) {
        if (client == null)
            return routeFactors;

        if (!StringExtension.isBlank(client.getId()))
            routeFactors.put(MetaKeys.CLIENT_ID, client.getId());

        addRouteFactors(routeFactors, client.getMetaMap(), MetaKeys.CLIENT);
        return routeFactors;
    }

    private Map<String, String> addRouteFactors(Map<String, String> routeFactors, Topic topic) {
        if (topic == null || StringExtension.isBlank(topic.getId()))
            return routeFactors;
        routeFactors.put(MetaKeys.TOPIC_ID, topic.getId());
        addRouteFactors(routeFactors, topic.getMetaMap(), MetaKeys.TOPIC);
        return routeFactors;
    }

    private Map<String, String> addRouteFactors(Map<String, String> routeFactors, ConsumerGroup consumerGroup) {
        if (consumerGroup == null || Util.isEmpty(consumerGroup.getId()))
            return routeFactors;
        routeFactors.put(MetaKeys.CONSUMER_GROUP_GROUP_NAME, consumerGroup.getId().getGroupName());
        routeFactors.put(MetaKeys.CONSUMER_GROUP_TOPIC_ID, consumerGroup.getId().getTopicId());
        addRouteFactors(routeFactors, consumerGroup.getMetaMap(), MetaKeys.CONSUMER_GROUP);
        return routeFactors;
    }

    private Map<String, String> addRouteFactors(Map<String, String> routeFactors, Map<String, String> map,
            String type) {
        if (map == null)
            return routeFactors;

        map.forEach((k, v) -> routeFactors.put(MetaKeys.constructMetaKey(type, k), v));
        return routeFactors;
    }

    private Route applyRouteRule(List<RouteRule> routeRules, Map<String, String> routeFactors, String topicId) {
        RouteRules.sort(routeRules);
        for (RouteRule routeRule : routeRules) {
            Route route = routeRule.apply(routeFactors);
            if (route != null) {
                Cluster cluster = _clusterRepository.getRecord(route.getClusterId());
                if (cluster == null) {
                    _logger.error("bad route rule: {}, cluster {} not exist.", routeRule, route.getClusterId());
                    continue;
                }

                return RouteRules.fillTopicId(route, topicId);
            }
        }

        return null;
    }

    public interface RouteRules {

        interface MetaKeys {

            String SEPARATOR = ".";
            String ID = "id";

            String CLIENT = "client";
            String CLIENT_ID = constructMetaKey(CLIENT, ID);

            String TOPIC = "topic";
            String TOPIC_ID = constructMetaKey(TOPIC, ID);

            String CONSUMER_GROUP = "consumerGroup";
            String CONSUMER_GROUP_GROUP_NAME = constructMetaKey(CONSUMER_GROUP, "groupName");
            String CONSUMER_GROUP_TOPIC_ID = constructMetaKey(CONSUMER_GROUP, "topicId");

            String RULE = "rule";
            String RULE_IS_GLOBAL = constructMetaKey(RULE, "isGlobal");

            static String constructMetaKey(String type, String key) {
                return type + SEPARATOR + key;
            }
        }

        Comparator<RouteRule> COMPARATOR = (r1, r2) -> {
            if (r1 == r2)
                return 0;

            if (r1 == null)
                return 1;

            if (r2 == null)
                return -1;

            int compareResult = r1.getPriority() - r2.getPriority();
            if (compareResult > 0)
                return -1;
            if (compareResult < 0)
                return 1;

            boolean isGlobal = isGlobal(r1);
            boolean isGlobal2 = isGlobal(r2);
            if (isGlobal && isGlobal2)
                return 0;
            if (isGlobal)
                return 1;
            if (isGlobal2)
                return -1;

            boolean forConsumerGroup = forConsumerGroup(r1);
            boolean forConsumerGroup2 = forConsumerGroup(r2);
            if (forConsumerGroup && !forConsumerGroup2)
                return -1;
            if (!forConsumerGroup && forConsumerGroup2)
                return 1;
            return 0;
        };

        static void sort(List<RouteRule> routeRules) {
            if (routeRules == null)
                return;

            routeRules.sort(COMPARATOR);
        }

        static String getTopicId(RouteRule routeRule) {
            return routeRule.getMeta().get(MetaKeys.TOPIC_ID);
        }

        static boolean forTopic(RouteRule routeRule) {
            return !StringExtension.isBlank(getTopicId(routeRule));
        }

        static ConsumerGroupId getConsumerGroupId(RouteRule routeRule) {
            String groupName = routeRule.getMeta().get(MetaKeys.CONSUMER_GROUP_GROUP_NAME);
            String topicId = routeRule.getMeta().get(MetaKeys.CONSUMER_GROUP_TOPIC_ID);
            if (StringExtension.isBlank(groupName) || StringExtension.isBlank(topicId))
                return null;
            return ConsumerGroupId.newBuilder().setGroupName(groupName).setTopicId(topicId).build();
        }

        static boolean forConsumerGroup(RouteRule routeRule) {
            String groupName = routeRule.getMeta().get(MetaKeys.CONSUMER_GROUP_GROUP_NAME);
            String topicId = routeRule.getMeta().get(MetaKeys.CONSUMER_GROUP_TOPIC_ID);
            return !StringExtension.isBlank(groupName) && !StringExtension.isBlank(topicId);
        }

        static boolean isGlobal(RouteRule routeRule) {
            String isGlobal = routeRule.getMeta().get(MetaKeys.RULE_IS_GLOBAL);
            return Objects.equals("true", isGlobal);
        }

        static boolean belongToTopic(RouteRule routeRule, String topicId) {
            if (isGlobal(routeRule))
                return true;

            String id = getTopicId(routeRule);
            return Objects.equals(topicId, id);
        }

        static boolean belongToConsumerGroup(RouteRule routeRule, ConsumerGroupId consumerGroupId) {
            if (isGlobal(routeRule))
                return true;

            if (belongToTopic(routeRule, consumerGroupId.getTopicId()))
                return true;

            ConsumerGroupId id = getConsumerGroupId(routeRule);
            return Objects.equals(consumerGroupId, id);
        }

        static Route fillTopicId(Route route, String topicId) {
            if (StringExtension.isBlank(route.getTopicId()))
                return route.toBuilder().setTopicId(topicId).build();

            return route;
        }

        static RouteRuleInfo getInfo(RouteRule routeRule) {
            RouteRuleInfo.Builder builder = RouteRuleInfo.newBuilder().setId(routeRule.getId())
                    .setPriority(routeRule.getPriority()).putAllMeta(routeRule.getMeta());
            if (routeRule instanceof CrrlRouteRule)
                builder.putMeta("rule", ((CrrlRouteRule) routeRule).getRule().toString());
            return builder.build();
        }

    }

}
