package com.ctrip.framework.kbear.route.crrl;

import java.util.Map;

/**
 * @author koqizhao
 *
 * Nov 14, 2018
 */
public class AndCriteria extends BinaryCriteria {

    public static final String OPERATOR = "and";

    public AndCriteria(CrrlCriteria leftOperand, CrrlCriteria rightOperand) {
        super(leftOperand, rightOperand);
    }

    @Override
    public boolean match(Map<String, String> routeFactors) {
        return getLeftOperand().match(routeFactors) && getRightOperand().match(routeFactors);
    }

    @Override
    protected String getOperator() {
        return OPERATOR;
    }

}
