package com.ctrip.framework.kbear.meta;

import java.util.List;

import org.mydotey.rpc.response.Response;
import org.mydotey.rpc.response.ResponseStatus;

public class FetchTopicsResponse implements Response {

    private ResponseStatus status;
    private List<Topic> topics;

    public ResponseStatus getStatus() {
        return status;
    }

    public void setStatus(ResponseStatus status) {
        this.status = status;
    }

    public List<Topic> getTopics() {
        return topics;
    }

    public void setTopics(List<Topic> topics) {
        this.topics = topics;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
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
        FetchTopicsResponse other = (FetchTopicsResponse) obj;
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
        return "FetchTopicsResponse [status=" + status + ", topics=" + topics + "]";
    }

}
