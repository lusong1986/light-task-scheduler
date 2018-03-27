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
 * http://*:8719/JT_192.168.0.101/job_gray_add_cmd?gray=green\&taskId=hello_2
 * 
 * @author lusong
 *
 */
public class AddJobGrayCommand implements HttpCmdProc {

	private final Logger LOGGER = LoggerFactory.getLogger(AddJobGrayCommand.class);

	private JobTrackerAppContext appContext;

	public AddJobGrayCommand(JobTrackerAppContext appContext) {
		this.appContext = appContext;
	}

	@Override
	public String nodeIdentity() {
		return appContext.getConfig().getIdentity();
	}

	@Override
	public String getCommand() {
		return HttpCmdNames.HTTP_CMD_ADD_JOB_GRAY;
	}

	@Override
	public HttpCmdResponse execute(HttpCmdRequest request) throws Exception {
		String gray = request.getParam("gray");
		if (StringUtils.isEmpty(gray)) {
			return HttpCmdResponse.newResponse(false, "gray should not be empty");
		} else if (!(gray.equals("BLUE") || gray.equals("GREEN"))) {
			return HttpCmdResponse.newResponse(false, "gray should be BLUE or GREEN");
		}

		String taskId = request.getParam("taskId");
		if (StringUtils.isEmpty(taskId)) {
			return HttpCmdResponse.newResponse(false, "taskId should not be empty");
		}

		appContext.getJobGrayFlag().add(taskId, gray);
		LOGGER.info("add job gray succeed, {} ,{}", taskId, gray);

		return HttpCmdResponse.newResponse(true, "add job gray succeed");
	}

}
