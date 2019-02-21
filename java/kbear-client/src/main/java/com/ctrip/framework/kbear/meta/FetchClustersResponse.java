package com.ctrip.framework.kbear.meta;

import java.util.List;

import org.mydotey.rpc.response.Response;
import org.mydotey.rpc.response.ResponseStatus;

public class FetchClustersResponse implements Response {

    private ResponseStatus status;
    private List<Cluster> clusters;

    public ResponseStatus getStatus() {
        return status;
    }

    public void setStatus(ResponseStatus status) {
        this.status = status;
    }

    public List<Cluster> getClusters() {
        return clusters;
    }

    public void setClusters(List<Cluster> clusters) {
        this.clusters = clusters;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((clusters == null) ? 0 : clusters.hashCode());
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
        FetchClustersResponse other = (FetchClustersResponse) obj;
        if (clusters == null) {
            if (other.clusters != null)
                return false;
        } else if (!clusters.equals(other.clusters))
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
        return "FetchClustersResponse [status=" + status + ", clusters=" + clusters + "]";
    }

}
