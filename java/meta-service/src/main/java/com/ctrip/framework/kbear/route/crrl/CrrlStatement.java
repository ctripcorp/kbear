package com.ctrip.framework.kbear.route.crrl;

import com.ctrip.framework.kbear.route.Route;

/**
 * @author koqizhao
 *
 * Nov 14, 2018
 */
public interface CrrlStatement {

    Route getRoute();

    CrrlCriteria getCriteria();

}
