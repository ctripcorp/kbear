package com.ctrip.framework.kbear.meta.repository;

import com.ctrip.framework.kbear.meta.Cluster;
import com.ctrip.framework.kbear.repository.Repository;

/**
 * @author koqizhao
 *
 * Nov 13, 2018
 */
public interface ClusterRepository extends Repository<String, Cluster> {

    String KEY_BOOTSTRAP_SERVERS = "bootstrap.servers";
    String KEY_ZOOKEEPER_CONNECT = "zookeeper.connect";

}
