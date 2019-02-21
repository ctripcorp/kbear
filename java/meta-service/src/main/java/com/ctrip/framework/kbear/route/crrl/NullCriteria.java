package com.ctrip.framework.kbear.route.crrl;

import java.util.Map;

/**
 * @author koqizhao
 *
 * Nov 14, 2018
 */
public class NullCriteria implements CrrlCriteria {

    @Override
    public boolean match(Map<String, String> routeFactors) {
        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;

        return this.getClass() == obj.getClass();
    }

    @Override
    public String toString() {
        return "";
    }

}
