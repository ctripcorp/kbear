package com.ctrip.framework.kbear.route.crrl;

import java.util.Map;
import java.util.Objects;

/**
 * @author koqizhao
 *
 * Nov 14, 2018
 */
public class EqualCriteria extends BasicCriteria {

    public static final String OPERATOR = "=";

    public EqualCriteria(String factorKey, String factorValue) {
        super(factorKey, factorValue);
    }

    @Override
    public boolean match(Map<String, String> routeFactors) {
        return Objects.equals(routeFactors.get(getFactorKey()), getFactorValue());
    }

    @Override
    protected String getOperator() {
        return OPERATOR;
    }

}
