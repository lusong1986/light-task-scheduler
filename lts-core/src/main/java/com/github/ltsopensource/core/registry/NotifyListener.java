package com.github.ltsopensource.core.registry;

import java.util.List;

import com.github.ltsopensource.core.cluster.Node;

/**
 * @author Robert HG (254963746@qq.com) on 5/17/15.
 */
public interface NotifyListener {

	void notify(NotifyEvent event, List<Node> nodes);

}
