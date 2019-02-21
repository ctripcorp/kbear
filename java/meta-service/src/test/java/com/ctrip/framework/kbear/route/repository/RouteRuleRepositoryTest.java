package com.ctrip.framework.kbear.route.repository;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.ctrip.framework.kbear.meta.ConsumerGroupId;
import com.ctrip.framework.kbear.meta.repository.ConsumerGroupRepositoryTest;
import com.ctrip.framework.kbear.meta.repository.TopicRepositoryTest;
import com.ctrip.framework.kbear.repository.Repository;
import com.ctrip.framework.kbear.repository.RepositoryTest;
import com.ctrip.framework.kbear.route.DefaultRouteService.RouteRules;
import com.ctrip.framework.kbear.route.DefaultRouteService.RouteRules.MetaKeys;
import com.ctrip.framework.kbear.route.RouteRule;
import com.ctrip.framework.kbear.route.crrl.CrrlRouteRule;
import com.google.common.collect.ImmutableMap;

/**
 * @author koqizhao
 *
 * Nov 28, 2018
 */
public class RouteRuleRepositoryTest extends RepositoryTest<String, RouteRule> {

    public static final String ROUTE_RULE_ID = "r100001";
    public static final String ROUTE_RULE_ID_2 = "r100002";
    public static final String ROUTE_RULE_ID_3 = "r100003";
    public static final String ROUTE_RULE_ID_4 = "r100004";
    public static final String ROUTE_RULE_ID_5 = "r100005";
    public static final String ROUTE_RULE_ID_6 = "r100006";
    public static final String UNKNOWN_ROUTE_RULE_ID = "unknown";

    public static final RouteRule ROUTE_RULE = new CrrlRouteRule(ROUTE_RULE_ID, 0,
            ImmutableMap.of(MetaKeys.RULE_IS_GLOBAL, "true"), "use cluster fws");
    public static final RouteRule ROUTE_RULE_2 = new CrrlRouteRule(ROUTE_RULE_ID_2, 0,
            ImmutableMap.of(MetaKeys.TOPIC_ID, TopicRepositoryTest.TOPIC_ID), "use cluster uat");
    public static final RouteRule ROUTE_RULE_3 = new CrrlRouteRule(ROUTE_RULE_ID_3, 1,
            ImmutableMap.of(MetaKeys.TOPIC_ID, TopicRepositoryTest.TOPIC_ID),
            "use cluster prod when client.id=100000001");
    public static final RouteRule ROUTE_RULE_4 = new CrrlRouteRule(ROUTE_RULE_ID_4, 0,
            ImmutableMap.of(MetaKeys.CONSUMER_GROUP_GROUP_NAME, ConsumerGroupRepositoryTest.CONSUMER_GROUP_NAME,
                    MetaKeys.CONSUMER_GROUP_TOPIC_ID, TopicRepositoryTest.TOPIC_ID),
            "use cluster prod");
    public static final RouteRule ROUTE_RULE_5 = new CrrlRouteRule(ROUTE_RULE_ID_5, 0,
            ImmutableMap.of(MetaKeys.CONSUMER_GROUP_GROUP_NAME, ConsumerGroupRepositoryTest.CONSUMER_GROUP_NAME_2,
                    MetaKeys.CONSUMER_GROUP_TOPIC_ID, TopicRepositoryTest.TOPIC_ID),
            "use cluster fws, topic fx.hellobom.string");
    public static final RouteRule ROUTE_RULE_6 = new CrrlRouteRule(ROUTE_RULE_ID_6, 1,
            ImmutableMap.of(MetaKeys.RULE_IS_GLOBAL, "true"), "use cluster uat when client.env=UAT");

    public static final List<RouteRule> ALL_ROUTE_RULES = new ArrayList<>(
            Arrays.asList(ROUTE_RULE, ROUTE_RULE_2, ROUTE_RULE_3, ROUTE_RULE_4, ROUTE_RULE_5, ROUTE_RULE_6));
    public static final List<RouteRule> TOPIC_ROUTE_RULES = new ArrayList<>(
            Arrays.asList(ROUTE_RULE, ROUTE_RULE_2, ROUTE_RULE_3, ROUTE_RULE_6));
    public static final List<RouteRule> TOPIC_2_ROUTE_RULES = new ArrayList<>(Arrays.asList(ROUTE_RULE, ROUTE_RULE_6));
    public static final List<RouteRule> CONSUMER_GROUP_ROUTE_RULES = new ArrayList<>(
            Arrays.asList(ROUTE_RULE, ROUTE_RULE_2, ROUTE_RULE_3, ROUTE_RULE_4, ROUTE_RULE_6));
    public static final List<RouteRule> CONSUMER_GROUP_2_ROUTE_RULES = new ArrayList<>(
            Arrays.asList(ROUTE_RULE, ROUTE_RULE_6));
    public static final List<RouteRule> CONSUMER_GROUP_3_ROUTE_RULES = new ArrayList<>(
            Arrays.asList(ROUTE_RULE, ROUTE_RULE_2, ROUTE_RULE_3, ROUTE_RULE_5, ROUTE_RULE_6));

    static {
        RouteRules.sort(ALL_ROUTE_RULES);
        RouteRules.sort(TOPIC_ROUTE_RULES);
        RouteRules.sort(TOPIC_2_ROUTE_RULES);
        RouteRules.sort(CONSUMER_GROUP_ROUTE_RULES);
        RouteRules.sort(CONSUMER_GROUP_2_ROUTE_RULES);
        RouteRules.sort(CONSUMER_GROUP_3_ROUTE_RULES);
    }

    @Parameters(name = "{index}: id={0}, record={1}, ids={2}, records={3}, all={4}, topicId={5}, topicRecords={6}, consumerGroupId={7}, consumerGroupRecords={8}")
    public static Collection<Object[]> data() {
        List<RouteRule> records = new ArrayList<>(Arrays.asList(ROUTE_RULE, ROUTE_RULE_2));
        RouteRules.sort(records);

        List<Object[]> parameterValues = new ArrayList<>();
        parameterValues.add(new Object[] { ROUTE_RULE_ID, ROUTE_RULE, Arrays.asList(ROUTE_RULE_ID),
                Arrays.asList(ROUTE_RULE), ALL_ROUTE_RULES, TopicRepositoryTest.TOPIC_ID, TOPIC_ROUTE_RULES,
                ConsumerGroupRepositoryTest.CONSUMER_GROUP_ID, CONSUMER_GROUP_ROUTE_RULES });
        parameterValues.add(new Object[] { ROUTE_RULE_ID, ROUTE_RULE, Arrays.asList(ROUTE_RULE_ID, ROUTE_RULE_ID_2),
                records, ALL_ROUTE_RULES, TopicRepositoryTest.TOPIC_ID_2, TOPIC_2_ROUTE_RULES,
                ConsumerGroupRepositoryTest.CONSUMER_GROUP_ID_2, CONSUMER_GROUP_2_ROUTE_RULES });
        parameterValues.add(new Object[] { UNKNOWN_ROUTE_RULE_ID, null, Collections.emptyList(),
                Collections.emptyList(), ALL_ROUTE_RULES, TopicRepositoryTest.TOPIC_ID, TOPIC_ROUTE_RULES,
                ConsumerGroupRepositoryTest.CONSUMER_GROUP_ID_3, CONSUMER_GROUP_3_ROUTE_RULES });
        return parameterValues;
    }

    @Parameter(5)
    public String _topicId;

    @Parameter(6)
    public List<RouteRule> _topicRecords;

    @Parameter(7)
    public ConsumerGroupId _consumerGroupId;

    @Parameter(8)
    public List<RouteRule> _consumerGroupRecords;

    @Override
    protected Repository<String, RouteRule> newRepository() {
        return new ConfigRouteRuleRepository();
    }

    @Override
    protected RouteRuleRepository getRepository() {
        return (RouteRuleRepository) super.getRepository();
    }

    @Test
    public void getRecordsByTopicIdTest() {
        List<RouteRule> topicRecords = getRepository().getRecords(_topicId);
        Assert.assertEquals(_topicRecords, topicRecords);
    }

    @Test
    public void getRecordsByConsumerGroupIdTest() {
        List<RouteRule> consumerGroupRecords = getRepository().getRecords(_consumerGroupId);
        Assert.assertEquals(_consumerGroupRecords, consumerGroupRecords);
    }

}
