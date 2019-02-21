package com.ctrip.framework.kbear.route.crrl;

import org.mydotey.java.StringExtension;

import com.ctrip.framework.kbear.route.Route;

/**
 * @author koqizhao
 *
 * Nov 14, 2018
 */
public class DefaultStatement implements CrrlStatement {

    private Route route;
    private CrrlCriteria criteria;

    public DefaultStatement(Route route, CrrlCriteria criteria) {
        super();
        this.route = route;
        this.criteria = criteria;
    }

    @Override
    public Route getRoute() {
        return route;
    }

    @Override
    public CrrlCriteria getCriteria() {
        return criteria;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((criteria == null) ? 0 : criteria.hashCode());
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
        DefaultStatement other = (DefaultStatement) obj;
        if (criteria == null) {
            if (other.criteria != null)
                return false;
        } else if (!criteria.equals(other.criteria))
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
        StringBuilder builder = new StringBuilder();
        builder.append("use cluster ").append(route.getClusterId());
        if (!StringExtension.isBlank(route.getTopicId()))
            builder.append(", topic ").append(route.getTopicId());
        if (!(criteria instanceof NullCriteria))
            builder.append(" when ").append(criteria);
        return builder.toString();
    }

}
