package com.ctrip.framework.kbear.route.crrl;

import java.util.Objects;

import org.javatuples.Pair;
import org.mydotey.java.StringExtension;

import com.ctrip.framework.kbear.route.Route;

/**
 * @author koqizhao
 *
 * Nov 14, 2018
 */
public class DefaultParser implements CrrlParser {

    @Override
    public CrrlStatement parse(String rule) {
        rule = rule.trim();

        rule = trimClause(rule, "use");

        Pair<String, String> pair = parseValue(rule, "cluster", "clusterId");
        String clusterId = pair.getValue0();
        rule = pair.getValue1();

        Route.Builder routeBuilder = Route.newBuilder().setClusterId(clusterId);

        Pair<String, Boolean> result = tryTrimComma(rule);
        rule = result.getValue0();
        if (result.getValue1()) {
            pair = parseValue(rule, "topic", "topicId");
            String topicId = pair.getValue0();
            routeBuilder.setTopicId(topicId);
            rule = pair.getValue1();
        }

        CrrlCriteria criteria;
        if (StringExtension.isBlank(rule))
            criteria = new NullCriteria();
        else {
            String criteriaString = trimClause(rule, "when");
            criteria = parseCriteria(criteriaString);
        }

        return new DefaultStatement(routeBuilder.build(), criteria);
    }

    protected String trimClause(String rule, String clause) {
        if (!rule.startsWith(clause + " ")) {
            String errorMessage = String.format("not starts with: '%s ', bad statement: '%s'", clause, rule);
            throw new CrrlStatmentException(errorMessage);
        }

        return rule.substring(clause.length()).trim();
    }

    protected Pair<String, Boolean> tryTrimComma(String rule) {
        if (!rule.startsWith(","))
            return new Pair<String, Boolean>(rule, false);

        return new Pair<String, Boolean>(rule.substring(1).trim(), true);
    }

    protected Pair<String, String> parseValue(String rule, String clause, String name) {
        rule = trimClause(rule, clause);
        return parseValue(rule, name);
    }

    protected Pair<String, String> parseValue(String rule, String name) {
        int index = rule.indexOf(",");
        if (index == -1)
            index = rule.indexOf(" ");

        String value = index == -1 ? rule : rule.substring(0, index);
        if (Objects.equals(value, "when")) {
            String errorMessage = String.format("no %s! bad statement: '%s'", name, rule);
            throw new CrrlStatmentException(errorMessage);
        }

        rule = index == -1 ? "" : rule.substring(index).trim();
        return new Pair<>(value, rule);
    }

    protected CrrlCriteria parseCriteria(String criteriaString) {
        int index = criteriaString.indexOf(AndCriteria.OPERATOR);
        if (index != -1) {
            String[] parts = criteriaString.split(AndCriteria.OPERATOR, 2);
            if (parts.length != 2) {
                String errorMessage = String.format("bad criteria: '%s'", criteriaString);
                throw new CrrlStatmentException(errorMessage);
            }

            CrrlCriteria left = parseCriteria(parts[0].trim());
            CrrlCriteria right = parseCriteria(parts[1].trim());
            return new AndCriteria(left, right);
        }

        index = criteriaString.indexOf(EqualCriteria.OPERATOR);
        if (index != -1) {
            String[] parts = criteriaString.split(EqualCriteria.OPERATOR, 2);
            if (parts.length != 2) {
                String errorMessage = String.format("bad criteria: '%s'", criteriaString);
                throw new CrrlStatmentException(errorMessage);
            }

            String metaKey = parts[0].trim();
            String metaValue = parts[1].trim();
            return new EqualCriteria(metaKey, metaValue);
        }

        String errorMessage = String.format("not supported: '%s'", criteriaString);
        throw new CrrlStatmentException(errorMessage);
    }

}
