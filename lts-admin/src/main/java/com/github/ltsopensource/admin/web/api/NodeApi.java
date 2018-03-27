package com.github.ltsopensource.admin.web.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.ltsopensource.admin.access.domain.NodeOnOfflineLog;
import com.github.ltsopensource.admin.cluster.BackendAppContext;
import com.github.ltsopensource.admin.request.NodeGroupRequest;
import com.github.ltsopensource.admin.request.NodeOnOfflineLogPaginationReq;
import com.github.ltsopensource.admin.request.NodePaginationReq;
import com.github.ltsopensource.admin.response.PaginationRsp;
import com.github.ltsopensource.admin.web.AbstractMVC;
import com.github.ltsopensource.admin.web.vo.RestfulResponse;
import com.github.ltsopensource.cmd.DefaultHttpCmd;
import com.github.ltsopensource.cmd.HttpCmdClient;
import com.github.ltsopensource.cmd.HttpCmdResponse;
import com.github.ltsopensource.core.cluster.Node;
import com.github.ltsopensource.core.cluster.NodeType;
import com.github.ltsopensource.core.cmd.HttpCmdNames;
import com.github.ltsopensource.core.commons.utils.CatUtils;
import com.github.ltsopensource.core.commons.utils.CollectionUtils;
import com.github.ltsopensource.core.domain.NodeGroupGetReq;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.queue.domain.NodeGroupPo;

/**
 * @author Robert HG (254963746@qq.com) on 5/11/15.
 */
@RestController
@RequestMapping("/node")
public class NodeApi extends AbstractMVC {

	private static final Logger LOGGER = LoggerFactory.getLogger(NodeApi.class);

	@Autowired
	private BackendAppContext appContext;

	class NodeExt extends Node {
		private Integer availableThread = 0;
		private Long timestamp = 0L;
		private String gray = "";
		private Boolean running = true;

		public NodeExt() {
			super();
		}

		public Integer getAvailableThread() {
			return availableThread;
		}

		public void setAvailableThread(Integer availableThread) {
			this.availableThread = availableThread;
		}

		public Long getTimestamp() {
			return timestamp;
		}

		public void setTimestamp(Long timestamp) {
			this.timestamp = timestamp;
		}

		public String getGray() {
			return gray;
		}

		public void setGray(String gray) {
			this.gray = gray;
		}

		public Boolean getRunning() {
			return running;
		}

		public void setRunning(Boolean running) {
			this.running = running;
		}

		public String toString() {
			return JSON.toJSONString(this);
		}
	}

	@RequestMapping("node-list-get")
	public RestfulResponse getNodeList(NodePaginationReq request) {
		RestfulResponse response = new RestfulResponse();
		request.setLimit(10000);
		PaginationRsp<Node> paginationRsp = appContext.getBackendRegistrySrv().getOnlineNodes(request);

		response.setSuccess(true);
		response.setResults(paginationRsp.getResults());

		final List<Node> nodes = paginationRsp.getRows();
		final Map<String, NodeExt> taskTrackerNodes = getTaskNodeMap(nodes);

		final List<NodeExt> nodeExts = new ArrayList<NodeExt>();
		if (nodes != null && nodes.size() > 0) {
			for (Node node : nodes) {
				NodeExt taskNode = taskTrackerNodes.get(node.getIdentity());
				if (taskNode == null) {
					taskNode = toNodeExt(node);
				}
				nodeExts.add(taskNode);
			}
		}
		response.setRows(nodeExts);

		return response;
	}

	private Map<String, NodeExt> getTaskNodeMap(final List<Node> nodes) {
		final Map<String, NodeExt> taskTrackerNodes = new HashMap<String, NodeExt>();
		if (nodes != null && nodes.size() > 0) {
			final JSONObject json = new JSONObject();
			for (Node node : nodes) {
				if (!node.getGroup().equals("lts")) {
					json.put(node.getIdentity(), node.getGroup());
				}
			}

			if (json.keySet().size() > 0) {
				DefaultHttpCmd httpCmd = new DefaultHttpCmd();
				httpCmd.setCommand(HttpCmdNames.HTTP_CMD_GET_TASKTRACKER_NODE);
				httpCmd.addParam("json", json.toJSONString());

				List<Node> jobTrackerNodeList = appContext.getNodeMemCacheAccess().getNodeByNodeType(
						NodeType.JOB_TRACKER);
				if (CollectionUtils.isNotEmpty(jobTrackerNodeList)) {
					for (Node jobTrackerNode : jobTrackerNodeList) {
						httpCmd.setNodeIdentity(jobTrackerNode.getIdentity());
						HttpCmdResponse responseHttp = HttpCmdClient.doGet(jobTrackerNode.getIp(),
								jobTrackerNode.getHttpCmdPort(), httpCmd);
						if (responseHttp.isSuccess()) {
							final String ret = responseHttp.getObj();

							taskTrackerNodes.putAll(getTaskTrackerNodes(nodes, ret));
							break;
						}
					}
				}
			}
		}

		return taskTrackerNodes;
	}

	private Map<String, NodeExt> getTaskTrackerNodes(final List<Node> nodes, final String ret) {
		final Map<String, NodeExt> taskTrackerNodes = new HashMap<String, NodeExt>();
		final JSONObject retJson = JSON.parseObject(ret);
		if (nodes != null && nodes.size() > 0) {
			for (Node node : nodes) {
				if (!node.getGroup().equals("lts")) {
					Object object = retJson.get(node.getIdentity());
					if (object != null) {
						try {
							final JSONObject taskTrackerJson = JSON.parseObject("" + object);
							if (taskTrackerJson != null) {
								final NodeExt taskTrackerAdminNode = toNodeExt(node);

								taskTrackerAdminNode.setTimestamp(taskTrackerJson.getLong("timestamp"));
								taskTrackerAdminNode.setRunning(taskTrackerJson.getBoolean("running"));
								taskTrackerAdminNode.setGray(taskTrackerJson.getString("gray"));
								taskTrackerAdminNode.setAvailableThread(taskTrackerJson.getInteger("availableThread"));

								taskTrackerNodes.put(node.getIdentity(), taskTrackerAdminNode);
							}
						} catch (Exception e) {
							LOGGER.warn("generate taskTrackerAdminNode warn:" + e.getMessage());
						}
					}
				}
			}
		}
		return taskTrackerNodes;
	}

	private NodeExt toNodeExt(final Node node) {
		final NodeExt nodeExt = new NodeExt();
		nodeExt.setAvailable(node.isAvailable());
		nodeExt.setClusterName(node.getClusterName());
		nodeExt.setCreateTime(node.getCreateTime());
		nodeExt.setGroup(node.getGroup());
		nodeExt.setHostName(node.getHostName());
		nodeExt.setHttpCmdPort(node.getHttpCmdPort());
		nodeExt.setIdentity(node.getIdentity());
		nodeExt.setIp(node.getIp());
		nodeExt.setJob(node.getJob());
		nodeExt.setListenNodeTypes(node.getListenNodeTypes());
		nodeExt.setNodeType(node.getNodeType());
		nodeExt.setPort(node.getPort());
		nodeExt.setThreads(node.getThreads());
		nodeExt.setAvailableThread(node.getThreads());
		return nodeExt;
	}

	@RequestMapping("registry-re-subscribe")
	public RestfulResponse reSubscribe() {
		RestfulResponse response = new RestfulResponse();

		appContext.getBackendRegistrySrv().reSubscribe();

		response.setSuccess(true);
		return response;
	}

	@RequestMapping("offline-tasktracker")
	public RestfulResponse offlineTaskTracker(String identity, String group) {
		RestfulResponse response = new RestfulResponse();

		NodePaginationReq request = new NodePaginationReq();
		request.setNodeGroup(group);
		request.setIdentity(identity);
		request.setLimit(10000);
		PaginationRsp<Node> paginationRsp = appContext.getBackendRegistrySrv().getOnlineNodes(request);
		if (paginationRsp != null && paginationRsp.getRows().size() > 0) {
			Node taskNode = paginationRsp.getRows().get(0);

			DefaultHttpCmd httpCmd = new DefaultHttpCmd();
			httpCmd.setCommand(HttpCmdNames.HTTP_CMD_JOB_PULL_MACHINE);
			httpCmd.addParam("op", "stop");
			httpCmd.setNodeIdentity(taskNode.getIdentity());
			HttpCmdResponse httpResponse = HttpCmdClient.doGet(taskNode.getIp(), taskNode.getHttpCmdPort(), httpCmd);
			if (httpResponse.isSuccess()) {
				LOGGER.info("stop " + taskNode.getIdentity() + " success.");
				response.setSuccess(true);
				return response;
			}
		}

		response.setMsg("offline failed.");
		response.setSuccess(false);
		return response;
	}

	@RequestMapping("online-tasktracker")
	public RestfulResponse onlineTaskTracker(String identity, String group) {
		RestfulResponse response = new RestfulResponse();

		NodePaginationReq request = new NodePaginationReq();
		request.setNodeGroup(group);
		request.setIdentity(identity);
		request.setLimit(10000);
		PaginationRsp<Node> paginationRsp = appContext.getBackendRegistrySrv().getOnlineNodes(request);
		if (paginationRsp != null && paginationRsp.getRows().size() > 0) {
			Node taskNode = paginationRsp.getRows().get(0);

			DefaultHttpCmd httpCmd = new DefaultHttpCmd();
			httpCmd.setCommand(HttpCmdNames.HTTP_CMD_JOB_PULL_MACHINE);
			httpCmd.addParam("op", "start");
			httpCmd.setNodeIdentity(taskNode.getIdentity());
			HttpCmdResponse httpResponse = HttpCmdClient.doGet(taskNode.getIp(), taskNode.getHttpCmdPort(), httpCmd);
			if (httpResponse.isSuccess()) {
				LOGGER.info("start " + taskNode.getIdentity() + " success.");
				response.setSuccess(true);
				return response;
			}
		}

		response.setMsg("online failed.");
		response.setSuccess(false);
		return response;
	}

	@RequestMapping("node-group-get")
	public RestfulResponse getNodeGroup(NodeGroupGetReq nodeGroupGetReq) {
		RestfulResponse response = new RestfulResponse();
		PaginationRsp<NodeGroupPo> paginationRsp = appContext.getNodeGroupStore().getNodeGroup(nodeGroupGetReq);

		response.setResults(paginationRsp.getResults());
		response.setRows(paginationRsp.getRows());
		response.setSuccess(true);
		return response;
	}

	@RequestMapping("node-group-add")
	public RestfulResponse addNodeGroup(NodeGroupRequest request) {
		RestfulResponse response = new RestfulResponse();
		appContext.getNodeGroupStore().addNodeGroup(request.getNodeType(), request.getNodeGroup());
		if (NodeType.TASK_TRACKER.equals(request.getNodeType())) {
			appContext.getExecutableJobQueue().createQueue(request.getNodeGroup());
		} else if (NodeType.JOB_CLIENT.equals(request.getNodeType())) {
			appContext.getJobFeedbackQueue().createQueue(request.getNodeGroup());
		}

		CatUtils.recordEvent(true, request.getNodeGroup(), "ADD_NODEGROUP");
		response.setSuccess(true);
		return response;
	}

	@RequestMapping("node-group-del")
	public RestfulResponse delNodeGroup(NodeGroupRequest request) {
		RestfulResponse response = new RestfulResponse();
		appContext.getNodeGroupStore().removeNodeGroup(request.getNodeType(), request.getNodeGroup());
		if (NodeType.TASK_TRACKER.equals(request.getNodeType())) {
			appContext.getExecutableJobQueue().removeQueue(request.getNodeGroup());
		} else if (NodeType.JOB_CLIENT.equals(request.getNodeType())) {
			appContext.getJobFeedbackQueue().removeQueue(request.getNodeGroup());
		}

		CatUtils.recordEvent(true, request.getNodeGroup(), "DEL_NODEGROUP");
		response.setSuccess(true);
		return response;
	}

	@RequestMapping("node-onoffline-log-get")
	public RestfulResponse getNodeOnOfflineLog(NodeOnOfflineLogPaginationReq request) {
		RestfulResponse response = new RestfulResponse();
		Long results = appContext.getBackendNodeOnOfflineLogAccess().count(request);
		response.setResults(results.intValue());
		if (results > 0) {
			List<NodeOnOfflineLog> rows = appContext.getBackendNodeOnOfflineLogAccess().select(request);
			response.setRows(rows);
		} else {
			response.setRows(new ArrayList<Object>(0));
		}
		response.setSuccess(true);
		return response;
	}
}
