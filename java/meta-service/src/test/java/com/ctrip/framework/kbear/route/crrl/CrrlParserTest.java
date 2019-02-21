package com.ctrip.framework.kbear.route.crrl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.ctrip.framework.kbear.route.Route;
import com.ctrip.framework.kbear.route.crrl.CrrlParser;
import com.ctrip.framework.kbear.route.crrl.CrrlStatement;
import com.ctrip.framework.kbear.route.crrl.DefaultParser;

/**
 * @author koqizhao
 *
 * Nov 14, 2018
 */
@RunWith(Parameterized.class)
public class CrrlParserTest {

    @Parameters(name = "{index}: rule={0}, route={1}")
    public static Collection<Object[]> data() {
        List<Object[]> parameterValues = new ArrayList<>();

        parameterValues.add(new Object[] { "use cluster c1, topic t1",
                Route.newBuilder().setClusterId("c1").setTopicId("t1").build() });

        parameterValues.add(new Object[] { "use cluster c1", Route.newBuilder().setClusterId("c1").build() });

        parameterValues.add(new Object[] { "use cluster c1, topic t1 when topic.id=t1",
                Route.newBuilder().setClusterId("c1").setTopicId("t1").build() });

        parameterValues.add(new Object[] { "use cluster c1, topic t1 when topic.id=t2",
                Route.newBuilder().setClusterId("c1").setTopicId("t1").build() });

        parameterValues.add(new Object[] { "use cluster c1, topic t1 when topic.id=t1 and client.id=c1",
                Route.newBuilder().setClusterId("c1").setTopicId("t1").build() });

        parameterValues.add(new Object[] { "use cluster c1 when topic.id=t1 and client.id=c1",
                Route.newBuilder().setClusterId("c1").build() });

        return parameterValues;
    }

    private CrrlParser _parser;

    @Parameter(0)
    public String rule;

    @Parameter(1)
    public Route route;

    public CrrlParserTest() {
        _parser = newParser();
    }

    @Test
    public void parseTest() {
        CrrlStatement statement = _parser.parse(rule);
        System.out.println(statement);
        Assert.assertEquals(route, statement.getRoute());
        Assert.assertEquals(rule, statement.toString());
    }

    protected CrrlParser newParser() {
        return new DefaultParser();
    }

}
