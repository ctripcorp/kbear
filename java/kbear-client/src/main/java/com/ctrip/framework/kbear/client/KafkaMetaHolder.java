package com.ctrip.framework.kbear.client;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.ctrip.framework.kbear.meta.Cluster;
import com.ctrip.framework.kbear.meta.ConsumerGroup;
import com.ctrip.framework.kbear.meta.ConsumerGroupId;
import com.ctrip.framework.kbear.meta.Topic;
import com.ctrip.framework.kbear.route.Route;

/**
 * @author koqizhao
 *
 * Dec 23, 2018
 */
public class KafkaMetaHolder implements Cloneable {

    private Map<String, Cluster> _clusters;
    private Map<String, Topic> _topics;
    private Map<ConsumerGroupId, ConsumerGroup> _consumerGroups;
    private Map<String, Route> _topicRoutes;
    private Map<ConsumerGroupId, Route> _consumerGroupRoutes;

    public KafkaMetaHolder() {
        _clusters = new HashMap<>();
        _topics = new HashMap<>();
        _consumerGroups = new HashMap<>();
        _topicRoutes = new HashMap<>();
        _consumerGroupRoutes = new HashMap<>();
    }

    public Map<String, Cluster> getClusters() {
        return _clusters;
    }

    public Map<String, Topic> getTopics() {
        return _topics;
    }

    public Map<ConsumerGroupId, ConsumerGroup> getConsumerGroups() {
        return _consumerGroups;
    }

    public Map<String, Route> getTopicRoutes() {
        return _topicRoutes;
    }

    public Map<ConsumerGroupId, Route> getConsumerGroupRoutes() {
        return _consumerGroupRoutes;
    }

    @Override
    public KafkaMetaHolder clone() {
        KafkaMetaHolder metaHolder = new KafkaMetaHolder();
        metaHolder._clusters.putAll(_clusters);
        metaHolder._topics.putAll(_topics);
        metaHolder._consumerGroups.putAll(_consumerGroups);
        metaHolder._topicRoutes.putAll(_topicRoutes);
        metaHolder._consumerGroupRoutes.putAll(_consumerGroupRoutes);
        return metaHolder;
    }

    public void immutable() {
        _clusters = Collections.unmodifiableMap(_clusters);
        _topics = Collections.unmodifiableMap(_topics);
        _consumerGroups = Collections.unmodifiableMap(_consumerGroups);
        _topicRoutes = Collections.unmodifiableMap(_topicRoutes);
        _consumerGroupRoutes = Collections.unmodifiableMap(_consumerGroupRoutes);
    }

}
