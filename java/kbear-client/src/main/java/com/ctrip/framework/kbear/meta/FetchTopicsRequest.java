package com.ctrip.framework.kbear.meta;

import java.util.List;

public class FetchTopicsRequest {

    private List<String> topicIds;

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
        FetchTopicsRequest other = (FetchTopicsRequest) obj;
        if (topicIds == null) {
            if (other.topicIds != null)
                return false;
        } else if (!topicIds.equals(other.topicIds))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "FetchTopicsRequest [topicIds=" + topicIds + "]";
    }

}
