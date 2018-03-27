package com.github.ltsopensource.zookeeper.lts;

/**
 * @author Robert HG (254963746@qq.com) on 2/18/16.
 */
public class ZkInterruptedException extends ZkException {

	private static final long serialVersionUID = -7146586841560712410L;

	public ZkInterruptedException() {
		super();
	}

	public ZkInterruptedException(String message) {
		super(message);
	}

	public ZkInterruptedException(String message, Throwable cause) {
		super(message, cause);
	}

	public ZkInterruptedException(Throwable cause) {
		super(cause);
	}
}
