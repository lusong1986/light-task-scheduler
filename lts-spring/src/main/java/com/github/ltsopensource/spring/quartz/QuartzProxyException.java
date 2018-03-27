package com.github.ltsopensource.spring.quartz;

/**
 * @author Robert HG (254963746@qq.com) on 3/27/16.
 */
public class QuartzProxyException extends RuntimeException {

	private static final long serialVersionUID = -4964502486624228957L;

	public QuartzProxyException() {
        super();
    }

    public QuartzProxyException(String message) {
        super(message);
    }

    public QuartzProxyException(String message, Throwable cause) {
        super(message, cause);
    }

    public QuartzProxyException(Throwable cause) {
        super(cause);
    }
}
