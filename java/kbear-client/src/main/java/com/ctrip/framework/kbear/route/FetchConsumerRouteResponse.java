package com.ctrip.framework.kbear.route;

import java.util.List;
import java.util.Map;

import org.mydotey.rpc.response.Response;
import org.mydotey.rpc.response.ResponseStatus;

import com.ctrip.framework.kbear.meta.Cluster;
import com.ctrip.framework.kbear.meta.ConsumerGroup;
import com.ctrip.framework.kbear.meta.Topic;

public class FetchConsumerRouteResponse implements Response {

    private ResponseStatus status;
    private List<ConsumerGroupIdRoutePair> consumerGroupIdRoutes;
    private Map<String, Cluster> clusters;
    private Map<String, Topic> topics;
    private List<ConsumerGroup> consumerGroups;

    public ResponseStatus getStatus() {
        return status;
    }

    public void setStatus(ResponseStatus status) {
        this.status = status;
    }

    public List<ConsumerGroupIdRoutePair> getConsumerGroupIdRoutes() {
        return consumerGroupIdRoutes;
    }

    public void setConsumerGroupIdRoutes(List<ConsumerGroupIdRoutePair> consumerGroupIdRoutes) {
        this.consumerGroupIdRoutes = consumerGroupIdRoutes;
    }

    public Map<String, Cluster> getClusters() {
        return clusters;
    }

    public void setClusters(Map<String, Cluster> clusters) {
        this.clusters = clusters;
    }

    public Map<String, Topic> getTopics() {
        return topics;
    }

    public void setTopics(Map<String, Topic> topics) {
        this.topics = topics;
    }

    public List<ConsumerGroup> getConsumerGroups() {
        return consumerGroups;
    }

    public void setConsumerGroups(List<ConsumerGroup> consumerGroups) {
        this.consumerGroups = consumerGroups;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((clusters == null) ? 0 : clusters.hashCode());
        result = prime * result + ((consumerGroupIdRoutes == null) ? 0 : consumerGroupIdRoutes.hashCode());
        result = prime * result + ((consumerGroups == null) ? 0 : consumerGroups.hashCode());
        result = prime * result + ((status == null) ? 0 : status.hashCode());
        result = prime * result + ((topics == null) ? 0 : topics.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        FetchConsumerRouteResponse other = (FetchConsumerRouteResponse) obj;
        if (clusters == null) {
            if (other.clusters != null)
                return false;
        } else if (!clusters.equals(other.clusters))
            return false;
        if (consumerGroupIdRoutes == null) {
            if (other.consumerGroupIdRoutes != null)
                return false;
        } else if (!consumerGroupIdRoutes.equals(other.consumerGroupIdRoutes))
            return false;
        if (consumerGroups == null) {
            if (other.consumerGroups != null)
                return false;
        } else if (!consumerGroups.equals(other.consumerGroups))
            return false;
        if (status == null) {
            if (other.status != null)
                return false;
        } else if (!status.equals(other.status))
            return false;
        if (topics == null) {
            if (other.topics != null)
                return false;
        } else if (!topics.equals(other.topics))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "FetchConsumerRouteResponse [status=" + status + ", consumerGroupIdRoutes=" + consumerGroupIdRoutes
                + ", clusters=" + clusters + ", topics=" + topics + ", consumerGroups=" + consumerGroups + "]";
    }

}
