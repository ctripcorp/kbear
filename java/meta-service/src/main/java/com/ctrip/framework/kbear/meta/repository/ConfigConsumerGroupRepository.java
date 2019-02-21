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
import com.ctrip.framework.kbear.meta.ConsumerGroup;
import com.ctrip.framework.kbear.meta.ConsumerGroupId;
import com.ctrip.framework.kbear.repository.AbstractConfigRepository;

/**
 * @author koqizhao
 *
 * Nov 13, 2018
 */
@Singleton
@Named()
@SuppressWarnings({ "unchecked", "rawtypes" })
public class ConfigConsumerGroupRepository extends AbstractConfigRepository<ConsumerGroupId, ConsumerGroup>
        implements ConsumerGroupRepository {

    private static Logger _logger = LoggerFactory.getLogger(ConfigConsumerGroupRepository.class);

    @Override
    public ConsumerGroup getRecord(ConsumerGroupId id) {
        return getAll().stream().filter(r -> Objects.equals(id, r.getId())).findFirst().orElse(null);
    }

    @Override
    public List<ConsumerGroup> getRecords(List<ConsumerGroupId> ids) {
        return getAll().stream().filter(r -> ids.contains(r.getId())).collect(Collectors.toList());
    }

    @Override
    protected String getRepositoryName() {
        return "consumer-groups";
    }

    @Override
    protected TypeConverter<List<Map<String, Object>>, List<ConsumerGroup>> getTypeConverter() {
        return Converter.DEFAULT;
    }

    @Override
    protected Function<List<ConsumerGroup>, List<ConsumerGroup>> getValueFilter() {
        return Filter.DEFAULT;
    }

    protected static class Converter extends AbstractTypeConverter<List<Map<String, Object>>, List<ConsumerGroup>> {

        public static final Converter DEFAULT = new Converter();

        public Converter() {
            super((Class) List.class, (Class) List.class);
        }

        @Override
        public List<ConsumerGroup> convert(List<Map<String, Object>> source) {
            List<ConsumerGroup> consumerGroups = new ArrayList<>();
            source.forEach(m -> {
                ConsumerGroup.Builder builder = ConsumerGroup.newBuilder();
                ConsumerGroupId.Builder idBuilder = ConsumerGroupId.newBuilder();

                m.entrySet().forEach(e -> {
                    switch (e.getKey()) {
                        case "groupName":
                            if (e.getValue() != null)
                                idBuilder.setGroupName(String.valueOf(e.getValue()));
                            break;
                        case "topicId":
                            if (e.getValue() != null)
                                idBuilder.setTopicId(String.valueOf(e.getValue()));
                            break;
                        case "meta":
                            Map<String, Object> meta = (Map<String, Object>) e.getValue();
                            builder.putAllMeta(Util.toMap(meta));
                            break;
                        default:
                            break;
                    }
                });

                consumerGroups.add(builder.setId(idBuilder.build()).build());
            });
            return consumerGroups.isEmpty() ? null : consumerGroups;
        }

    }

    protected static class Filter implements Function<List<ConsumerGroup>, List<ConsumerGroup>> {

        public static final Filter DEFAULT = new Filter();

        @Override
        public List<ConsumerGroup> apply(List<ConsumerGroup> t) {
            List<ConsumerGroup> results = new ArrayList<>();
            t.forEach(c -> {
                if (isValid(c))
                    results.add(c);
                else
                    _logger.error("bad consumerGroup config: " + c);
            });

            return results.isEmpty() ? null : results;
        }

        protected boolean isValid(ConsumerGroup consumerGroup) {
            if (Util.isEmpty(consumerGroup.getId()))
                return false;

            return true;
        }

    }

}
