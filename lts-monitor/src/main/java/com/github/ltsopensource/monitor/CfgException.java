package com.github.ltsopensource.monitor;

/**
 * @author Robert HG (254963746@qq.com) on 3/10/16.
 */
public class CfgException extends RuntimeException {

	private static final long serialVersionUID = 1524068873727354302L;

	public CfgException(String message) {
        super(message);
    }

    public CfgException(String message, Throwable cause) {
        super(message, cause);
    }

    public CfgException(Throwable cause) {
        super(cause);
    }
}
