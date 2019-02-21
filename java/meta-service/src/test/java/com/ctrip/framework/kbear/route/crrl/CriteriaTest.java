package com.ctrip.framework.kbear.route.crrl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.ImmutableMap;

/**
 * @author koqizhao
 *
 * Nov 14, 2018
 */
@RunWith(Parameterized.class)
public class CriteriaTest {

    @Parameters(name = "{index}: criteria={0}, matchFactors={1}, notMatchFactors={2}")
    public static Collection<Object[]> data() {
        List<Object[]> parameterValues = new ArrayList<>();

        parameterValues.add(new Object[] { new NullCriteria(), ImmutableMap.of("client.id", "10000"), null });
        parameterValues.add(new Object[] { new NullCriteria(), ImmutableMap.of("topic.id", "xxx"), null });
        parameterValues.add(new Object[] { new NullCriteria(),
                ImmutableMap.of("consumerGroup.groupName", "xxx", "consumerGroup.topicId", "yyy"), null });

        parameterValues.add(new Object[] { new EqualCriteria("topic.id", "xxx"), ImmutableMap.of("topic.id", "xxx"),
                ImmutableMap.of("topic.id", "yyy") });
        parameterValues.add(new Object[] { new EqualCriteria("consumerGroup.topicId", "xxx"),
                ImmutableMap.of("consumerGroup.topicId", "xxx"), ImmutableMap.of("consumerGroup.topicId", "yyy") });

        parameterValues.add(new Object[] {
                new AndCriteria(new EqualCriteria("client.id", "10000"), new EqualCriteria("topic.id", "xxx")),
                ImmutableMap.of("topic.id", "xxx", "client.id", "10000"), ImmutableMap.of("topic.id", "yyy") });
        parameterValues.add(new Object[] {
                new AndCriteria(new EqualCriteria("client.id", "10000"), new EqualCriteria("topic.id", "xxx")),
                ImmutableMap.of("topic.id", "xxx", "client.id", "10000"), ImmutableMap.of("client.id", "10000") });
        parameterValues.add(new Object[] {
                new AndCriteria(new EqualCriteria("client.id", "10000"), new EqualCriteria("topic.id", "xxx")),
                ImmutableMap.of("topic.id", "xxx", "client.id", "10000"),
                ImmutableMap.of("consumerGroup.groupName", "xxx") });

        return parameterValues;
    }

    @Parameter(0)
    public CrrlCriteria criteria;

    @Parameter(1)
    public Map<String, String> matchFactors;

    @Parameter(2)
    public Map<String, String> notMatchFactors;

    @Test
    public void criteriaTest() {
        if (matchFactors != null)
            Assert.assertTrue(criteria.match(matchFactors));

        if (notMatchFactors != null)
            Assert.assertFalse(criteria.match(notMatchFactors));
    }

}
