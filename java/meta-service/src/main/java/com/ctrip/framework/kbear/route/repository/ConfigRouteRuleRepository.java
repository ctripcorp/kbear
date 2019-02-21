package com.ctrip.framework.kbear.route.repository;

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
import com.ctrip.framework.kbear.meta.ConsumerGroupId;
import com.ctrip.framework.kbear.repository.AbstractConfigRepository;
import com.ctrip.framework.kbear.route.RouteRule;
import com.ctrip.framework.kbear.route.DefaultRouteService.RouteRules;
import com.ctrip.framework.kbear.route.crrl.CrrlRouteRule;

/**
 * @author koqizhao
 *
 * Nov 13, 2018
 */
@Singleton
@Named()
@SuppressWarnings({ "rawtypes", "unchecked" })
public class ConfigRouteRuleRepository extends AbstractConfigRepository<String, RouteRule>
        implements RouteRuleRepository {

    private static Logger _logger = LoggerFactory.getLogger(ConfigRouteRuleRepository.class);

    @Override
    public RouteRule getRecord(String id) {
        return getAll().stream().filter(r -> Objects.equals(id, r.getId())).findFirst().orElse(null);
    }

    @Override
    public List<RouteRule> getRecords(List<String> ids) {
        return getAll().stream().filter(r -> ids.contains(r.getId())).collect(Collectors.toList());
    }

    @Override
    public List<RouteRule> getRecords(String topicId) {
        return getAll().stream().filter(rr -> RouteRules.belongToTopic(rr, topicId)).collect(Collectors.toList());
    }

    @Override
    public List<RouteRule> getRecords(ConsumerGroupId consumerGroupId) {
        return getAll().stream().filter(rr -> RouteRules.belongToConsumerGroup(rr, consumerGroupId))
                .collect(Collectors.toList());
    }

    @Override
    protected String getRepositoryName() {
        return "route-rules";
    }

    @Override
    protected TypeConverter<List<Map<String, Object>>, List<RouteRule>> getTypeConverter() {
        return Converter.DEFAULT;
    }

    @Override
    protected Function<List<RouteRule>, List<RouteRule>> getValueFilter() {
        return Filter.DEFAULT;
    }

    protected static class Converter extends AbstractTypeConverter<List<Map<String, Object>>, List<RouteRule>> {

        public static final Converter DEFAULT = new Converter();

        public Converter() {
            super((Class) List.class, (Class) List.class);
        }

        @Override
        public List<RouteRule> convert(List<Map<String, Object>> source) {
            List<RouteRule> routeRules = new ArrayList<>();
            source.forEach(m -> {
                RouteRule routeRule = convert(m);
                if (routeRule != null)
                    routeRules.add(routeRule);
            });
            RouteRules.sort(routeRules);
            return routeRules.isEmpty() ? null : routeRules;
        }

        private RouteRule convert(Map<String, Object> map) {
            try {
                String id = (String) map.get("id");
                Integer priority = (Integer) map.get("priority");
                if (priority == null)
                    priority = 0;
                Map<String, Object> meta = (Map<String, Object>) map.get("meta");
                String rule = (String) map.get("rule");
                return new CrrlRouteRule(id, priority, Util.toMap(meta), rule);
            } catch (Exception e) {
                _logger.error("Configuration has error, cannot convert to RouteRule: " + map, e);
                return null;
            }
        }

    }

    protected static class Filter implements Function<List<RouteRule>, List<RouteRule>> {

        public static final Filter DEFAULT = new Filter();

        @Override
        public List<RouteRule> apply(List<RouteRule> t) {
            List<RouteRule> results = new ArrayList<>();
            t.forEach(c -> {
                if (isValid(c))
                    results.add(c);
                else
                    _logger.error("bad routeRule: {}", c);
            });

            return results.isEmpty() ? null : results;
        }

        protected boolean isValid(RouteRule routeRule) {
            if (routeRule.getId() == null || routeRule.getId().isEmpty())
                return false;

            if (RouteRules.forTopic(routeRule) || RouteRules.forConsumerGroup(routeRule)
                    || RouteRules.isGlobal(routeRule))
                return true;

            return false;
        }

    }

}
