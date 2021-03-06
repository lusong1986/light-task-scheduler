package com.github.ltsopensource.alarm;

/**
 * @author Robert HG (254963746@qq.com) on 2/17/16.
 */
public class AlarmNotifyException extends RuntimeException {

	private static final long serialVersionUID = -3391171843313154451L;

	public AlarmNotifyException() {
        super();
    }

    public AlarmNotifyException(String message) {
        super(message);
    }

    public AlarmNotifyException(String message, Throwable cause) {
        super(message, cause);
    }

    public AlarmNotifyException(Throwable cause) {
        super(cause);
    }
}
