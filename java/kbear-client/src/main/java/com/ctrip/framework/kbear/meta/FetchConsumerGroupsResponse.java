package com.ctrip.framework.kbear.meta;

import java.util.List;

import org.mydotey.rpc.response.Response;
import org.mydotey.rpc.response.ResponseStatus;

public class FetchConsumerGroupsResponse implements Response {

    private ResponseStatus status;
    private List<ConsumerGroup> consumerGroups;

    public ResponseStatus getStatus() {
        return status;
    }

    public void setStatus(ResponseStatus status) {
        this.status = status;
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
        result = prime * result + ((consumerGroups == null) ? 0 : consumerGroups.hashCode());
        result = prime * result + ((status == null) ? 0 : status.hashCode());
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
        FetchConsumerGroupsResponse other = (FetchConsumerGroupsResponse) obj;
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
        return true;
    }

    @Override
    public String toString() {
        return "FetchConsumerGroupsResponse [status=" + status + ", consumerGroups=" + consumerGroups + "]";
    }

}
