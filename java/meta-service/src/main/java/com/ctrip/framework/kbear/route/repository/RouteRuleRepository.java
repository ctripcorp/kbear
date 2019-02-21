package com.ctrip.framework.kbear.route.repository;

import java.util.List;

import com.ctrip.framework.kbear.meta.ConsumerGroupId;
import com.ctrip.framework.kbear.repository.Repository;
import com.ctrip.framework.kbear.route.RouteRule;

/**
 * @author koqizhao
 *
 * Nov 13, 2018
 */
public interface RouteRuleRepository extends Repository<String, RouteRule> {

    List<RouteRule> getRecords(String topicId);

    List<RouteRule> getRecords(ConsumerGroupId consumerGroupId);

}
