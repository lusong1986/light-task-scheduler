package com.github.ltsopensource.jobtracker.cmd;

import java.util.Iterator;
import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.ltsopensource.cmd.HttpCmdProc;
import com.github.ltsopensource.cmd.HttpCmdRequest;
import com.github.ltsopensource.cmd.HttpCmdResponse;
import com.github.ltsopensource.core.cmd.HttpCmdNames;
import com.github.ltsopensource.core.commons.utils.StringUtils;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.jobtracker.domain.JobTrackerAppContext;
import com.github.ltsopensource.jobtracker.domain.TaskTrackerNode;

/**
 * 查询tasktracker的信息
 *
 * @author lusong
 */
public class GetTaskTrackerNodeHttpCmd implements HttpCmdProc {

	private static final Logger LOGGER = LoggerFactory.getLogger(GetTaskTrackerNodeHttpCmd.class);

	private JobTrackerAppContext appContext;

	public GetTaskTrackerNodeHttpCmd(JobTrackerAppContext appContext) {
		this.appContext = appContext;
	}

	@Override
	public String nodeIdentity() {
		return appContext.getConfig().getIdentity();
	}

	@Override
	public String getCommand() {
		return HttpCmdNames.HTTP_CMD_GET_TASKTRACKER_NODE;
	}

	@Override
	public HttpCmdResponse execute(HttpCmdRequest request) throws Exception {
		HttpCmdResponse response = new HttpCmdResponse();
		response.setSuccess(false);

		String taskTrackerIds = request.getParam("json");
		LOGGER.info("query taskTrackerIds" + taskTrackerIds);
		if (StringUtils.isEmpty(taskTrackerIds)) {
			response.setMsg("taskTrackerIds can not be null");
			return response;
		}

		try {
			final JSONObject ret = new JSONObject();

			final JSONObject taskTrackerIdsJson = JSON.parseObject(taskTrackerIds);
			Set<String> keySet = taskTrackerIdsJson.keySet();
			Iterator<String> it = keySet.iterator();
			while (it.hasNext()) {
				final String identity = it.next();
				final String nodeGroup = "" + taskTrackerIdsJson.get(identity);
				if (identity.startsWith("TT_")) {
					final TaskTrackerNode taskTrackerNode = appContext.getTaskTrackerManager()
							.getTaskTrackerNodeWithOffline(nodeGroup, identity);
					if (taskTrackerNode != null) {
						final JSONObject newjson = new JSONObject();
						newjson.put("availableThread", taskTrackerNode.getAvailableThread().intValue());
						newjson.put("gray", taskTrackerNode.getGray());
						newjson.put("nodeGroup", taskTrackerNode.getNodeGroup());
						newjson.put("running", taskTrackerNode.getRunning().get());
						newjson.put("timestamp", taskTrackerNode.getTimestamp());
						ret.put(identity, newjson);
					}
				}
			}

			response.setObj(ret.toJSONString());
			LOGGER.info("get tasktracker node succeed " + ret.toJSONString());

			response.setSuccess(true);
		} catch (Exception e) {
			LOGGER.error("get tasktracker node error, message:", e);
			response.setMsg(e.getMessage());
		}
		return response;
	}

}
