package com.ctrip.framework.kbear.route;

import com.ctrip.framework.kbear.meta.ConsumerGroupId;

public class ConsumerGroupIdRoutePair {

    private ConsumerGroupId consumerGroupId;
    private Route route;

    public ConsumerGroupId getConsumerGroupId() {
        return consumerGroupId;
    }

    public void setConsumerGroupId(ConsumerGroupId consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
    }

    public Route getRoute() {
        return route;
    }

    public void setRoute(Route route) {
        this.route = route;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((consumerGroupId == null) ? 0 : consumerGroupId.hashCode());
        result = prime * result + ((route == null) ? 0 : route.hashCode());
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
        ConsumerGroupIdRoutePair other = (ConsumerGroupIdRoutePair) obj;
        if (consumerGroupId == null) {
            if (other.consumerGroupId != null)
                return false;
        } else if (!consumerGroupId.equals(other.consumerGroupId))
            return false;
        if (route == null) {
            if (other.route != null)
                return false;
        } else if (!route.equals(other.route))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "ConsumerGroupIdRoutePair [consumerGroupId=" + consumerGroupId + ", route=" + route + "]";
    }

}
