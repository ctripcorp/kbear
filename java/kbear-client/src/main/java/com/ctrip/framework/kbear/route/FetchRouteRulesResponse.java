package com.ctrip.framework.kbear.route;

import java.util.List;

import org.mydotey.rpc.response.Response;
import org.mydotey.rpc.response.ResponseStatus;

public class FetchRouteRulesResponse implements Response {

    private ResponseStatus status;
    private List<RouteRuleInfo> routeRules;

    public ResponseStatus getStatus() {
        return status;
    }

    public void setStatus(ResponseStatus status) {
        this.status = status;
    }

    public List<RouteRuleInfo> getRouteRules() {
        return routeRules;
    }

    public void setRouteRules(List<RouteRuleInfo> routeRules) {
        this.routeRules = routeRules;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((routeRules == null) ? 0 : routeRules.hashCode());
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
        FetchRouteRulesResponse other = (FetchRouteRulesResponse) obj;
        if (routeRules == null) {
            if (other.routeRules != null)
                return false;
        } else if (!routeRules.equals(other.routeRules))
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
        return "FetchRouteRulesResponse [status=" + status + ", routeRules=" + routeRules + "]";
    }

}
