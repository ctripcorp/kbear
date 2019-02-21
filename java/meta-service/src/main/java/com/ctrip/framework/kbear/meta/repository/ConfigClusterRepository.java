package com.ctrip.framework.kbear.meta.repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Named;
import javax.inject.Singleton;

import org.mydotey.scf.type.AbstractTypeConverter;
import org.mydotey.scf.type.TypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.framework.kbear.Util;
import com.ctrip.framework.kbear.meta.Cluster;
import com.ctrip.framework.kbear.repository.AbstractConfigRepository;

/**
 * @author koqizhao
 *
 * Nov 13, 2018
 */
@Singleton
@Named()
@SuppressWarnings({ "unchecked", "rawtypes" })
public class ConfigClusterRepository extends AbstractConfigRepository<String, Cluster> implements ClusterRepository {

    private static Logger _logger = LoggerFactory.getLogger(ConfigClusterRepository.class);

    @Override
    public Cluster getRecord(String id) {
        return getAll().stream().filter(r -> Objects.equals(id, r.getId())).findFirst().orElse(null);
    }

    @Override
    public List<Cluster> getRecords(List<String> ids) {
        return getAll().stream().filter(r -> ids.contains(r.getId())).collect(Collectors.toList());
    }

    @Override
    protected String getRepositoryName() {
        return "clusters";
    }

    @Override
    protected TypeConverter<List<Map<String, Object>>, List<Cluster>> getTypeConverter() {
        return Converter.DEFAULT;
    }

    @Override
    protected Function<List<Cluster>, List<Cluster>> getValueFilter() {
        return Filter.DEFAULT;
    }

    protected static class Converter extends AbstractTypeConverter<List<Map<String, Object>>, List<Cluster>> {

        public static final Converter DEFAULT = new Converter();

        public Converter() {
            super((Class) List.class, (Class) List.class);
        }

        @Override
        public List<Cluster> convert(List<Map<String, Object>> source) {
            List<Cluster> clusters = new ArrayList<>();
            source.forEach(m -> {
                Cluster.Builder builder = Cluster.newBuilder();
                m.entrySet().forEach(e -> {
                    switch (e.getKey()) {
                        case "id":
                            if (e.getValue() != null)
                                builder.setId(String.valueOf(e.getValue()));
                            break;
                        case "meta":
                            Map<String, Object> meta = (Map<String, Object>) e.getValue();
                            builder.putAllMeta(Util.toMap(meta));
                            break;
                        default:
                            break;
                    }
                });

                clusters.add(builder.build());
            });
            return clusters.isEmpty() ? null : clusters;
        }

    }

    protected static class Filter implements Function<List<Cluster>, List<Cluster>> {

        public static final Filter DEFAULT = new Filter();

        @Override
        public List<Cluster> apply(List<Cluster> t) {
            List<Cluster> results = new ArrayList<>();
            t.forEach(c -> {
                if (isValid(c))
                    results.add(c);
                else
                    _logger.error("bad cluster config: " + c);
            });

            return results.isEmpty() ? null : results;
        }

        protected boolean isValid(Cluster cluster) {
            if (cluster.getId() == null || cluster.getId().isEmpty())
                return false;

            String zkConnect = cluster.getMetaMap().get(KEY_ZOOKEEPER_CONNECT);
            if (zkConnect == null || zkConnect.isEmpty())
                return false;

            String bootstrapServers = cluster.getMetaMap().get(KEY_BOOTSTRAP_SERVERS);
            if (bootstrapServers == null || bootstrapServers.isEmpty())
                return false;

            return true;
        }

    }

}
