package com.github.ltsopensource.store.jdbc.exception;

/**
 * 
 * @author lusong
 *
 */
public class SameTaskNameException extends RuntimeException {

	private static final long serialVersionUID = 5633679422452188620L;

	public SameTaskNameException() {
		super();
	}

	public SameTaskNameException(String message) {
		super(message);
	}

	public SameTaskNameException(String message, Throwable cause) {
		super(message, cause);
	}

	public SameTaskNameException(Throwable cause) {
		super(cause);
	}

}
