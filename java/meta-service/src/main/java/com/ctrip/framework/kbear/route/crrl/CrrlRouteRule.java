package com.ctrip.framework.kbear.route.crrl;

import java.util.Map;
import java.util.Objects;

import com.ctrip.framework.kbear.route.AbstractRoutRule;
import com.ctrip.framework.kbear.route.Route;

/**
 * @author koqizhao
 *
 * Nov 14, 2018
 */
public class CrrlRouteRule extends AbstractRoutRule {

    private CrrlStatement rule;

    public CrrlRouteRule(String id, int priority, Map<String, String> meta, String rule) {
        super(id, priority, meta);

        Objects.requireNonNull(rule, "rule is null");

        this.rule = new DefaultParser().parse(rule);
    }

    public CrrlStatement getRule() {
        return rule;
    }

    @Override
    public Route apply(Map<String, String> routeFactors) {
        return rule.getCriteria().match(routeFactors) ? rule.getRoute() : null;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((rule == null) ? 0 : rule.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        CrrlRouteRule other = (CrrlRouteRule) obj;
        if (rule == null) {
            if (other.rule != null)
                return false;
        } else if (!rule.equals(other.rule))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "CrrlRouteRule [id=" + getId() + ", priority=" + getPriority() + ", meta=" + getMeta() + ", rule='"
                + rule + "']";
    }

}
