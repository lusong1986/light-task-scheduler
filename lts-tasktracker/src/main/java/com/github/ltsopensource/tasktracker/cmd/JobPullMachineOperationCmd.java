package com.github.ltsopensource.tasktracker.cmd;

import com.github.ltsopensource.cmd.HttpCmdProc;
import com.github.ltsopensource.cmd.HttpCmdRequest;
import com.github.ltsopensource.cmd.HttpCmdResponse;
import com.github.ltsopensource.core.cmd.HttpCmdNames;
import com.github.ltsopensource.core.commons.utils.CatUtils;
import com.github.ltsopensource.core.commons.utils.StringUtils;
import com.github.ltsopensource.tasktracker.domain.TaskTrackerAppContext;

/**
 * 用于让tasktracker停止或者启动拉取Job
 * @author Robert HG (254963746@qq.com) on 3/13/16.
 */
public class JobPullMachineOperationCmd implements HttpCmdProc {

	private TaskTrackerAppContext appContext;

	public JobPullMachineOperationCmd(TaskTrackerAppContext appContext) {
		this.appContext = appContext;
	}

	@Override
	public String nodeIdentity() {
		return appContext.getConfig().getIdentity();
	}

	@Override
	public String getCommand() {
		return HttpCmdNames.HTTP_CMD_JOB_PULL_MACHINE;
	}

	@Override
	public HttpCmdResponse execute(HttpCmdRequest request) throws Exception {
		String op = request.getParam("op");
		if (StringUtils.isEmpty(op)) {
			return HttpCmdResponse.newResponse(false, "op can't be empty");
		}

		if ("start".equals(op)) {
			appContext.getJobPullMachine().start();
			CatUtils.recordEvent(true, appContext.getConfig().getIdentity(), "JOB_PULLMACHINE_START");
		} else if ("stop".equals(op)) {
			appContext.getJobPullMachine().stop();
			CatUtils.recordEvent(true, appContext.getConfig().getIdentity(), "JOB_PULLMACHINE_STOP");
		} else {
			return HttpCmdResponse.newResponse(false, "op is wrong");
		}

		return HttpCmdResponse.newResponse(true, "Execute Job Pull Command success");
	}
}
