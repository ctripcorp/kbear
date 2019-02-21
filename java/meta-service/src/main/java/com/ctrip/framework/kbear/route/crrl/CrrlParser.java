package com.ctrip.framework.kbear.route.crrl;

/**
 * @author koqizhao
 *
 * Nov 14, 2018
 */
public interface CrrlParser {

    CrrlStatement parse(String rule);

}
