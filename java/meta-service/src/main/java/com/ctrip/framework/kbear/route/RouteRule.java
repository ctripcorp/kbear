package com.ctrip.framework.kbear.route;

import java.util.Map;

/**
 * @author koqizhao
 *
 * Nov 14, 2018
 */
public interface RouteRule {

    String getId();

    int getPriority();

    Map<String, String> getMeta();

    Route apply(Map<String, String> routeFactors);

}
