package com.ctrip.framework.kbear.route;

import java.util.Map;

import org.mydotey.rpc.response.Response;
import org.mydotey.rpc.response.ResponseStatus;

import com.ctrip.framework.kbear.meta.Cluster;
import com.ctrip.framework.kbear.meta.Topic;

public class FetchProducerRouteResponse implements Response {

    private ResponseStatus status;
    private Map<String, Route> topicIdRoutes;
    private Map<String, Cluster> clusters;
    private Map<String, Topic> topics;

    public ResponseStatus getStatus() {
        return status;
    }

    public void setStatus(ResponseStatus status) {
        this.status = status;
    }

    public Map<String, Route> getTopicIdRoutes() {
        return topicIdRoutes;
    }

    public void setTopicIdRoutes(Map<String, Route> topicIdRoutes) {
        this.topicIdRoutes = topicIdRoutes;
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((clusters == null) ? 0 : clusters.hashCode());
        result = prime * result + ((status == null) ? 0 : status.hashCode());
        result = prime * result + ((topicIdRoutes == null) ? 0 : topicIdRoutes.hashCode());
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
        FetchProducerRouteResponse other = (FetchProducerRouteResponse) obj;
        if (clusters == null) {
            if (other.clusters != null)
                return false;
        } else if (!clusters.equals(other.clusters))
            return false;
        if (status == null) {
            if (other.status != null)
                return false;
        } else if (!status.equals(other.status))
            return false;
        if (topicIdRoutes == null) {
            if (other.topicIdRoutes != null)
                return false;
        } else if (!topicIdRoutes.equals(other.topicIdRoutes))
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
        return "FetchProducerRouteResponse [status=" + status + ", topicIdRoutes=" + topicIdRoutes + ", clusters="
                + clusters + ", topics=" + topics + "]";
    }

}
