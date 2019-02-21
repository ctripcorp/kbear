package com.ctrip.framework.kbear.route;

import java.util.List;

import com.ctrip.framework.kbear.meta.ConsumerGroupId;

public class FetchConsumerRouteRequest {

    private Client client;
    private List<ConsumerGroupId> consumerGroupIds;

    public Client getClient() {
        return client;
    }

    public void setClient(Client client) {
        this.client = client;
    }

    public List<ConsumerGroupId> getConsumerGroupIds() {
        return consumerGroupIds;
    }

    public void setConsumerGroupIds(List<ConsumerGroupId> consumerGroupIds) {
        this.consumerGroupIds = consumerGroupIds;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((client == null) ? 0 : client.hashCode());
        result = prime * result + ((consumerGroupIds == null) ? 0 : consumerGroupIds.hashCode());
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
        FetchConsumerRouteRequest other = (FetchConsumerRouteRequest) obj;
        if (client == null) {
            if (other.client != null)
                return false;
        } else if (!client.equals(other.client))
            return false;
        if (consumerGroupIds == null) {
            if (other.consumerGroupIds != null)
                return false;
        } else if (!consumerGroupIds.equals(other.consumerGroupIds))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "FetchConsumerRouteRequest [client=" + client + ", consumerGroupIds=" + consumerGroupIds + "]";
    }

}
