package com.ctrip.framework.kbear.route;

import java.util.HashMap;
import java.util.Map;

public class Client implements Cloneable {

    private String id;
    private Map<String, String> meta;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Map<String, String> getMeta() {
        return meta;
    }

    public void setMeta(Map<String, String> meta) {
        this.meta = meta;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((meta == null) ? 0 : meta.hashCode());
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
        Client other = (Client) obj;
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
        return true;
    }

    @Override
    public String toString() {
        return "Client [id=" + id + ", meta=" + meta + "]";
    }

    @Override
    public Client clone() {
        try {
            Client obj = (Client) super.clone();
            obj.meta = meta == null ? null : new HashMap<>(meta);
            return obj;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

}
