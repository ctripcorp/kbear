package com.ctrip.framework.kbear.route.crrl;

/**
 * @author koqizhao
 *
 * Nov 14, 2018
 */
public abstract class BinaryCriteria implements CrrlCriteria {

    private CrrlCriteria _leftOperand;
    private CrrlCriteria _rightOperand;

    public BinaryCriteria(CrrlCriteria leftOperand, CrrlCriteria rightOperand) {
        _leftOperand = leftOperand;
        _rightOperand = rightOperand;
    }

    public CrrlCriteria getLeftOperand() {
        return _leftOperand;
    }

    public CrrlCriteria getRightOperand() {
        return _rightOperand;
    }

    protected abstract String getOperator();

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_leftOperand == null) ? 0 : _leftOperand.hashCode());
        result = prime * result + ((_rightOperand == null) ? 0 : _rightOperand.hashCode());
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
        BinaryCriteria other = (BinaryCriteria) obj;
        if (_leftOperand == null) {
            if (other._leftOperand != null)
                return false;
        } else if (!_leftOperand.equals(other._leftOperand))
            return false;
        if (_rightOperand == null) {
            if (other._rightOperand != null)
                return false;
        } else if (!_rightOperand.equals(other._rightOperand))
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
        return String.format("%s %s %s", getLeftOperand(), getOperator(), getRightOperand());
    }

}
