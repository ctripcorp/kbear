package com.ctrip.framework.kbear.route.crrl;

/**
 * @author koqizhao
 *
 * Nov 14, 2018
 */
public class CrrlStatmentException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public CrrlStatmentException() {
        super();
    }

    public CrrlStatmentException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public CrrlStatmentException(String message, Throwable cause) {
        super(message, cause);
    }

    public CrrlStatmentException(String message) {
        super(message);
    }

    public CrrlStatmentException(Throwable cause) {
        super(cause);
    }

}
