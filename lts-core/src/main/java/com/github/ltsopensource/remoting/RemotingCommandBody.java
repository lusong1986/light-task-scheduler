package com.github.ltsopensource.remoting;

import java.io.Serializable;

import com.github.ltsopensource.remoting.exception.RemotingCommandFieldCheckException;

/**
 * RemotingCommand中自定义字段反射对象的公共接口
 */
public interface RemotingCommandBody extends Serializable {

	public void checkFields() throws RemotingCommandFieldCheckException;
}
