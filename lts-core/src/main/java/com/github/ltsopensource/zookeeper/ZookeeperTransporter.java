package com.github.ltsopensource.zookeeper;

import com.github.ltsopensource.core.cluster.Config;
import com.github.ltsopensource.core.constant.ExtConfig;
import com.github.ltsopensource.core.spi.SPI;

@SPI(key = ExtConfig.ZK_CLIENT_KEY, dftValue = "zkclient")
public interface ZookeeperTransporter {

	ZkClient connect(Config config);

}
