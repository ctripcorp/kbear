package com.ctrip.framework.kbear.route;

import java.util.Map;

/**
 * @author koqizhao
 *
 * Nov 14, 2018
 */
public abstract class AbstractRoutRule implements RouteRule {

    private String id;
    private int priority;
    private Map<String, String> meta;

    public AbstractRoutRule(String id, int priority, Map<String, String> meta) {
        this.id = id;
        this.priority = priority;
        this.meta = meta;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public Map<String, String> getMeta() {
        return meta;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((meta == null) ? 0 : meta.hashCode());
        result = prime * result + priority;
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
        AbstractRoutRule other = (AbstractRoutRule) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (meta == null) {
            if (other.meta != null)
                return false;
        } else if (!meta.equals(other.meta))
            return false;
        if (priority != other.priority)
            return false;
        return true;
    }

}
