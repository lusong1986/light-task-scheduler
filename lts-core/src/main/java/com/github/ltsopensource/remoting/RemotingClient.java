package com.github.ltsopensource.remoting;

import java.util.concurrent.ExecutorService;

import com.github.ltsopensource.remoting.exception.RemotingConnectException;
import com.github.ltsopensource.remoting.exception.RemotingException;
import com.github.ltsopensource.remoting.exception.RemotingSendRequestException;
import com.github.ltsopensource.remoting.exception.RemotingTimeoutException;
import com.github.ltsopensource.remoting.exception.RemotingTooMuchRequestException;
import com.github.ltsopensource.remoting.protocol.RemotingCommand;

/**
 * 远程通信，Client接口
 */
public interface RemotingClient {

	void start() throws RemotingException;

	/**
	 * 同步调用
	 */
	RemotingCommand invokeSync(final String addr, final RemotingCommand request, final long timeoutMillis)
			throws InterruptedException, RemotingConnectException, RemotingSendRequestException,
			RemotingTimeoutException;

	/**
	 * 异步调用
	 */
	void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMillis,
			final AsyncCallback asyncCallback) throws InterruptedException, RemotingConnectException,
			RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

	/**
	 * 单向调用
	 */
	void invokeOneway(final String addr, final RemotingCommand request, final long timeoutMillis)
			throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException,
			RemotingTimeoutException, RemotingSendRequestException;

	/**
	 * 注册处理器
	 */
	void registerProcessor(final int requestCode, final RemotingProcessor processor, final ExecutorService executor);

	/**
	 * 注册默认处理器
	 */
	void registerDefaultProcessor(final RemotingProcessor processor, final ExecutorService executor);

	void shutdown();
}
