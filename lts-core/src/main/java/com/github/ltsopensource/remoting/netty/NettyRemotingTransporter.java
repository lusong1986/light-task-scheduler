package com.github.ltsopensource.remoting.netty;

import com.github.ltsopensource.core.AppContext;
import com.github.ltsopensource.remoting.RemotingClient;
import com.github.ltsopensource.remoting.RemotingClientConfig;
import com.github.ltsopensource.remoting.RemotingServer;
import com.github.ltsopensource.remoting.RemotingServerConfig;
import com.github.ltsopensource.remoting.RemotingTransporter;

/**
 * @author Robert HG (254963746@qq.com) on 11/6/15.
 */
public class NettyRemotingTransporter implements RemotingTransporter {

	@Override
	public RemotingServer getRemotingServer(AppContext appContext, RemotingServerConfig remotingServerConfig) {
		return new NettyRemotingServer(appContext, remotingServerConfig);
	}

	@Override
	public RemotingClient getRemotingClient(AppContext appContext, RemotingClientConfig remotingClientConfig) {
		return new NettyRemotingClient(appContext, remotingClientConfig);
	}
}
