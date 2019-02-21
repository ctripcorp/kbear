package com.ctrip.framework.kbear.route.crrl;

import java.util.Map;

/**
 * @author koqizhao
 *
 * Nov 14, 2018
 */
public interface CrrlCriteria {

    boolean match(Map<String, String> routeFactors);

}
