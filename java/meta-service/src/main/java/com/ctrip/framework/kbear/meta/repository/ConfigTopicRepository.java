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
import com.ctrip.framework.kbear.meta.Topic;
import com.ctrip.framework.kbear.repository.AbstractConfigRepository;

/**
 * @author koqizhao
 *
 * Nov 13, 2018
 */
@Singleton
@Named()
@SuppressWarnings({ "unchecked", "rawtypes" })
public class ConfigTopicRepository extends AbstractConfigRepository<String, Topic> implements TopicRepository {

    private static Logger _logger = LoggerFactory.getLogger(ConfigTopicRepository.class);

    @Override
    public Topic getRecord(String id) {
        return getAll().stream().filter(r -> Objects.equals(id, r.getId())).findFirst().orElse(null);
    }

    @Override
    public List<Topic> getRecords(List<String> ids) {
        return getAll().stream().filter(r -> ids.contains(r.getId())).collect(Collectors.toList());
    }

    @Override
    protected String getRepositoryName() {
        return "topics";
    }

    @Override
    protected TypeConverter<List<Map<String, Object>>, List<Topic>> getTypeConverter() {
        return Converter.DEFAULT;
    }

    @Override
    protected Function<List<Topic>, List<Topic>> getValueFilter() {
        return Filter.DEFAULT;
    }

    protected static class Converter extends AbstractTypeConverter<List<Map<String, Object>>, List<Topic>> {

        public static final Converter DEFAULT = new Converter();

        public Converter() {
            super((Class) List.class, (Class) List.class);
        }

        @Override
        public List<Topic> convert(List<Map<String, Object>> source) {
            List<Topic> topics = new ArrayList<>();
            source.forEach(m -> {
                Topic.Builder builder = Topic.newBuilder();
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

                topics.add(builder.build());
            });
            return topics.isEmpty() ? null : topics;
        }

    }

    protected static class Filter implements Function<List<Topic>, List<Topic>> {

        public static final Filter DEFAULT = new Filter();

        @Override
        public List<Topic> apply(List<Topic> t) {
            List<Topic> results = new ArrayList<>();
            t.forEach(c -> {
                if (isValid(c))
                    results.add(c);
                else
                    _logger.error("bad topic config: " + c);
            });

            return results.isEmpty() ? null : results;
        }

        protected boolean isValid(Topic topic) {
            if (topic.getId() == null || topic.getId().isEmpty())
                return false;

            return true;
        }

    }

}
