package com.ctrip.framework.kbear.route;

import java.util.List;

public class FetchProducerRouteRequest {

    private Client client;
    private List<String> topicIds;

    public Client getClient() {
        return client;
    }

    public void setClient(Client client) {
        this.client = client;
    }

    public List<String> getTopicIds() {
        return topicIds;
    }

    public void setTopicIds(List<String> topicIds) {
        this.topicIds = topicIds;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((client == null) ? 0 : client.hashCode());
        result = prime * result + ((topicIds == null) ? 0 : topicIds.hashCode());
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
        FetchProducerRouteRequest other = (FetchProducerRouteRequest) obj;
        if (client == null) {
            if (other.client != null)
                return false;
        } else if (!client.equals(other.client))
            return false;
        if (topicIds == null) {
            if (other.topicIds != null)
                return false;
        } else if (!topicIds.equals(other.topicIds))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "FetchProducerRouteRequest [client=" + client + ", topicIds=" + topicIds + "]";
    }

}
