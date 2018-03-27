package com.github.ltsopensource.store.jdbc.exception;

/**
 * @author Robert HG (254963746@qq.com) on 3/26/15.
 */
public class DupEntryException extends JdbcException {

	private static final long serialVersionUID = -892640664831963530L;

	public DupEntryException() {
		super();
	}

	public DupEntryException(String message) {
		super(message);
	}

	public DupEntryException(String message, Throwable cause) {
		super(message, cause);
	}

	public DupEntryException(Throwable cause) {
		super(cause);
	}
}
