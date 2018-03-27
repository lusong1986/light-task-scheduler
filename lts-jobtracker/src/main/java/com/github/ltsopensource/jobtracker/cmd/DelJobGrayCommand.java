package com.github.ltsopensource.jobtracker.cmd;

import com.github.ltsopensource.cmd.HttpCmdProc;
import com.github.ltsopensource.cmd.HttpCmdRequest;
import com.github.ltsopensource.cmd.HttpCmdResponse;
import com.github.ltsopensource.core.cmd.HttpCmdNames;
import com.github.ltsopensource.core.commons.utils.StringUtils;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.jobtracker.domain.JobTrackerAppContext;

/**
 * 
 * http://*:8719/JT_192.168.0.101/job_gray_del_cmd?taskId=hello_2
 * 
 * @author lusong
 *
 */
public class DelJobGrayCommand implements HttpCmdProc {

	private final Logger LOGGER = LoggerFactory.getLogger(DelJobGrayCommand.class);

	private JobTrackerAppContext appContext;

	public DelJobGrayCommand(JobTrackerAppContext appContext) {
		this.appContext = appContext;
	}

	@Override
	public String nodeIdentity() {
		return appContext.getConfig().getIdentity();
	}

	@Override
	public String getCommand() {
		return HttpCmdNames.HTTP_CMD_DEL_JOB_GRAY;
	}

	@Override
	public HttpCmdResponse execute(HttpCmdRequest request) throws Exception {
		String taskId = request.getParam("taskId");
		if (StringUtils.isEmpty(taskId)) {
			return HttpCmdResponse.newResponse(false, "taskId should not be empty");
		}

		appContext.getJobGrayFlag().remove(taskId);
		LOGGER.info("delete job gray succeed, {} ,{}", taskId);

		return HttpCmdResponse.newResponse(true, "delete job gray succeed");
	}

}
