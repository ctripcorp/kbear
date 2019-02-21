package com.ctrip.framework.kbear.meta;

import java.util.List;

public class FetchClustersRequest {

    private List<String> clusterIds;

    public List<String> getClusterIds() {
        return clusterIds;
    }

    public void setClusterIds(List<String> clusterIds) {
        this.clusterIds = clusterIds;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((clusterIds == null) ? 0 : clusterIds.hashCode());
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
        FetchClustersRequest other = (FetchClustersRequest) obj;
        if (clusterIds == null) {
            if (other.clusterIds != null)
                return false;
        } else if (!clusterIds.equals(other.clusterIds))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "FetchClustersRequest [clusterIds=" + clusterIds + "]";
    }

}
