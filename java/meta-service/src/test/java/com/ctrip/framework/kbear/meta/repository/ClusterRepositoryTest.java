package com.ctrip.framework.kbear.meta.repository;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.runners.Parameterized.Parameters;

import com.ctrip.framework.kbear.meta.Cluster;
import com.ctrip.framework.kbear.repository.Repository;
import com.ctrip.framework.kbear.repository.RepositoryTest;

/**
 * @author koqizhao
 *
 * Nov 28, 2018
 */
public class ClusterRepositoryTest extends RepositoryTest<String, Cluster> {

    public static final String CLUSTER_ID = "fws";
    public static final String CLUSTER_ID_2 = "uat";
    public static final String CLUSTER_ID_3 = "prod";
    public static final String UNKNOWN_CLUSTER_ID = "unknown";

    public static final Cluster CLUSTER = Cluster.newBuilder().setId(CLUSTER_ID)
            .putMeta("bootstrap.servers", "10.2.74.6:9092,10.2.73.254:9092,10.2.73.255:9092")
            .putMeta("zookeeper.connect", "10.2.7.137:2181,10.2.7.138:2181,10.2.7.139:2181").build();
    public static final Cluster CLUSTER_2 = Cluster.newBuilder().setId(CLUSTER_ID_2)
            .putMeta("bootstrap.servers", "10.2.27.123:9092,10.2.27.124:9092,10.2.27.125:9092")
            .putMeta("zookeeper.connect", "10.2.27.123:2181,10.2.27.124:2181,10.2.27.125:2181").build();
    public static final Cluster CLUSTER_3 = Cluster.newBuilder().setId(CLUSTER_ID_3)
            .putMeta("bootstrap.servers", "10.28.133.27:9092,10.28.135.127:9092,10.28.135.128:9092")
            .putMeta("zookeeper.connect", "10.8.113.113:2181,10.8.113.114:2181,10.8.113.115:2181").build();

    public static final List<Cluster> ALL_CLUSTERS = Arrays.asList(CLUSTER, CLUSTER_2, CLUSTER_3);

    @Parameters(name = "{index}: id={0}, record={1}, ids={2}, records={3}, all={4}")
    public static Collection<Object[]> data() {
        List<Object[]> parameterValues = new ArrayList<>();
        parameterValues.add(
                new Object[] { CLUSTER_ID, CLUSTER, Arrays.asList(CLUSTER_ID), Arrays.asList(CLUSTER), ALL_CLUSTERS });
        parameterValues.add(new Object[] { CLUSTER_ID, CLUSTER, Arrays.asList(CLUSTER_ID, CLUSTER_ID_2),
                Arrays.asList(CLUSTER, CLUSTER_2), ALL_CLUSTERS });
        parameterValues.add(new Object[] { UNKNOWN_CLUSTER_ID, null, Collections.emptyList(), Collections.emptyList(),
                ALL_CLUSTERS });
        return parameterValues;
    }

    @Override
    protected Repository<String, Cluster> newRepository() {
        return new ConfigClusterRepository();
    }

}
