package com.ctrip.framework.kbear.meta.repository;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.runners.Parameterized.Parameters;

import com.ctrip.framework.kbear.meta.Topic;
import com.ctrip.framework.kbear.repository.Repository;
import com.ctrip.framework.kbear.repository.RepositoryTest;

/**
 * @author koqizhao
 *
 * Nov 28, 2018
 */
public class TopicRepositoryTest extends RepositoryTest<String, Topic> {

    public static final String TOPIC_ID = "fx.kafka.demo.hello.run";
    public static final String TOPIC_ID_2 = "fx.hellobom.string";
    public static final String UNKNOWN_TOPIC_ID = "unknown";

    public static final Topic TOPIC = Topic.newBuilder().setId(TOPIC_ID).putMeta("bu", "framework").build();
    public static final Topic TOPIC_2 = Topic.newBuilder().setId(TOPIC_ID_2).putMeta("bu", "basebiz").build();
    public static final Topic UNKNOWN_TOPIC = Topic.newBuilder().setId(UNKNOWN_TOPIC_ID).build();

    public static final List<Topic> ALL_TOPICS = Arrays.asList(TOPIC, TOPIC_2);

    @Parameters(name = "{index}: id={0}, record={1}, ids={2}, records={3}, all={4}")
    public static Collection<Object[]> data() {
        List<Object[]> parameterValues = new ArrayList<>();
        parameterValues
                .add(new Object[] { TOPIC_ID, TOPIC, Arrays.asList(TOPIC_ID), Arrays.asList(TOPIC), ALL_TOPICS });
        parameterValues.add(new Object[] { TOPIC_ID, TOPIC, Arrays.asList(TOPIC_ID, TOPIC_ID_2),
                Arrays.asList(TOPIC, TOPIC_2), ALL_TOPICS });
        parameterValues.add(
                new Object[] { UNKNOWN_TOPIC_ID, null, Collections.emptyList(), Collections.emptyList(), ALL_TOPICS });
        return parameterValues;
    }

    @Override
    protected Repository<String, Topic> newRepository() {
        return new ConfigTopicRepository();
    }

}
