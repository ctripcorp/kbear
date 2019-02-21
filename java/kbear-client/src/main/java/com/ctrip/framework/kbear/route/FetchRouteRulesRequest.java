package com.ctrip.framework.kbear.route;

import java.util.List;

public class FetchRouteRulesRequest {

    private List<String> routeRuleIds;

    public List<String> getRouteRuleIds() {
        return routeRuleIds;
    }

    public void setRouteRuleIds(List<String> routeRuleIds) {
        this.routeRuleIds = routeRuleIds;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((routeRuleIds == null) ? 0 : routeRuleIds.hashCode());
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
        FetchRouteRulesRequest other = (FetchRouteRulesRequest) obj;
        if (routeRuleIds == null) {
            if (other.routeRuleIds != null)
                return false;
        } else if (!routeRuleIds.equals(other.routeRuleIds))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "FetchRouteRulesRequest [routeRuleIds=" + routeRuleIds + "]";
    }

}
