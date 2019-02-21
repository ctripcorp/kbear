package com.ctrip.framework.kbear.route.crrl;

/**
 * @author koqizhao
 *
 * Nov 14, 2018
 */
public abstract class BasicCriteria implements CrrlCriteria {

    private String _factorKey;
    private String _factorValue;

    public BasicCriteria(String factorKey, String factorValue) {
        _factorKey = factorKey;
        _factorValue = factorValue;
    }

    protected String getFactorKey() {
        return _factorKey;
    }

    protected String getFactorValue() {
        return _factorValue;
    }

    protected abstract String getOperator();

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_factorKey == null) ? 0 : _factorKey.hashCode());
        result = prime * result + ((_factorValue == null) ? 0 : _factorValue.hashCode());
        result = prime * result + ((getOperator() == null) ? 0 : getOperator().hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BasicCriteria other = (BasicCriteria) obj;
        if (_factorKey == null) {
            if (other._factorKey != null)
                return false;
        } else if (!_factorKey.equals(other._factorKey))
            return false;
        if (_factorValue == null) {
            if (other._factorValue != null)
                return false;
        } else if (!_factorValue.equals(other._factorValue))
            return false;
        if (getOperator() == null) {
            if (other.getOperator() != null)
                return false;
        } else if (!getOperator().equals(other.getOperator()))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return String.format("%s%s%s", getFactorKey(), getOperator(), getFactorValue());
    }
}
