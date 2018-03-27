package com.github.ltsopensource.core.groovy;

/**
 * @author Robert HG (254963746@qq.com) on 11/11/15.
 */
public class GroovyException extends Exception{

	private static final long serialVersionUID = -1082312300034422142L;

	public GroovyException() {
        super();
    }

    public GroovyException(String message) {
        super(message);
    }

    public GroovyException(String message, Throwable cause) {
        super(message, cause);
    }

    public GroovyException(Throwable cause) {
        super(cause);
    }
}
