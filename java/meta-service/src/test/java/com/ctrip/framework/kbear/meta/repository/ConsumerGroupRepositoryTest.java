package com.ctrip.framework.kbear.meta.repository;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.runners.Parameterized.Parameters;

import com.ctrip.framework.kbear.meta.ConsumerGroup;
import com.ctrip.framework.kbear.meta.ConsumerGroupId;
import com.ctrip.framework.kbear.repository.Repository;
import com.ctrip.framework.kbear.repository.RepositoryTest;

/**
 * @author koqizhao
 *
 * Nov 28, 2018
 */
public class ConsumerGroupRepositoryTest extends RepositoryTest<ConsumerGroupId, ConsumerGroup> {

    public static final String CONSUMER_GROUP_NAME = "fx.kafka.demo.hello.run.consumer";
    public static final String CONSUMER_GROUP_NAME_2 = "fx.hellobom.string.consumer";
    public static final String UNKNOWN_CONSUMER_GROUP_NAME = "unknown";

    public static final ConsumerGroupId CONSUMER_GROUP_ID = ConsumerGroupId.newBuilder()
            .setGroupName(CONSUMER_GROUP_NAME).setTopicId(TopicRepositoryTest.TOPIC_ID).build();
    public static final ConsumerGroupId CONSUMER_GROUP_ID_2 = ConsumerGroupId.newBuilder()
            .setGroupName(CONSUMER_GROUP_NAME_2).setTopicId(TopicRepositoryTest.TOPIC_ID_2).build();
    public static final ConsumerGroupId CONSUMER_GROUP_ID_3 = ConsumerGroupId.newBuilder()
            .setGroupName(CONSUMER_GROUP_NAME_2).setTopicId(TopicRepositoryTest.TOPIC_ID).build();
    public static final ConsumerGroupId UNKNOWN_CONSUMER_GROUP_ID = ConsumerGroupId.newBuilder()
            .setGroupName(UNKNOWN_CONSUMER_GROUP_NAME).setTopicId(TopicRepositoryTest.UNKNOWN_TOPIC_ID).build();

    public static final ConsumerGroup CONSUMER_GROUP = ConsumerGroup.newBuilder().setId(CONSUMER_GROUP_ID)
            .putMeta("bu", "framework").build();
    public static final ConsumerGroup CONSUMER_GROUP_2 = ConsumerGroup.newBuilder().setId(CONSUMER_GROUP_ID_2)
            .putMeta("bu", "basebiz").build();
    public static final ConsumerGroup CONSUMER_GROUP_3 = ConsumerGroup.newBuilder().setId(CONSUMER_GROUP_ID_3)
            .putMeta("bu", "basebiz").build();
    public static final ConsumerGroup UNKNOWN_CONSUMER_GROUP = ConsumerGroup.newBuilder()
            .setId(UNKNOWN_CONSUMER_GROUP_ID).build();

    public static final List<ConsumerGroup> ALL_CONSUMER_GROUPS = Arrays.asList(CONSUMER_GROUP, CONSUMER_GROUP_2,
            CONSUMER_GROUP_3);

    @Parameters(name = "{index}: id={0}, record={1}, ids={2}, records={3}, all={4}")
    public static Collection<Object[]> data() {
        List<Object[]> parameterValues = new ArrayList<>();
        parameterValues.add(new Object[] { CONSUMER_GROUP_ID, CONSUMER_GROUP, Arrays.asList(CONSUMER_GROUP_ID),
                Arrays.asList(CONSUMER_GROUP), ALL_CONSUMER_GROUPS });
        parameterValues.add(
                new Object[] { CONSUMER_GROUP_ID, CONSUMER_GROUP, Arrays.asList(CONSUMER_GROUP_ID, CONSUMER_GROUP_ID_2),
                        Arrays.asList(CONSUMER_GROUP, CONSUMER_GROUP_2), ALL_CONSUMER_GROUPS });
        parameterValues.add(new Object[] { UNKNOWN_CONSUMER_GROUP_ID, null, Collections.emptyList(),
                Collections.emptyList(), ALL_CONSUMER_GROUPS });
        return parameterValues;
    }

    @Override
    protected Repository<ConsumerGroupId, ConsumerGroup> newRepository() {
        return new ConfigConsumerGroupRepository();
    }

}
