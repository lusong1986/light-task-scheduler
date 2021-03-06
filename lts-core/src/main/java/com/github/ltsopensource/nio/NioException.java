package com.github.ltsopensource.nio;

/**
 * @author Robert HG (254963746@qq.com) on 1/9/16.
 */
public class NioException extends RuntimeException {

	private static final long serialVersionUID = -649908426562923649L;

	public NioException() {
        super();
    }

    public NioException(String message) {
        super(message);
    }

    public NioException(String message, Throwable cause) {
        super(message, cause);
    }

    public NioException(Throwable cause) {
        super(cause);
    }
}
