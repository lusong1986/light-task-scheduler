package com.github.ltsopensource.jobtracker.cmd;

import com.github.ltsopensource.cmd.HttpCmdProc;
import com.github.ltsopensource.cmd.HttpCmdRequest;
import com.github.ltsopensource.cmd.HttpCmdResponse;
import com.github.ltsopensource.core.cmd.HttpCmdNames;
import com.github.ltsopensource.core.commons.utils.StringUtils;
import com.github.ltsopensource.core.domain.JobDependency;
import com.github.ltsopensource.core.json.JSON;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.jobtracker.domain.JobTrackerAppContext;
import com.github.ltsopensource.queue.domain.JobDependencyPo;

public class AddJobDependencyHttpCmd implements HttpCmdProc {

	private final Logger LOGGER = LoggerFactory.getLogger(AddJobDependencyHttpCmd.class);

	private JobTrackerAppContext appContext;

	public AddJobDependencyHttpCmd(JobTrackerAppContext appContext) {
		this.appContext = appContext;
	}

	@Override
	public String nodeIdentity() {
		return appContext.getConfig().getIdentity();
	}

	@Override
	public String getCommand() {
		return HttpCmdNames.HTTP_CMD_ADD_JOB_DEPENDENCY;
	}

	@Override
	public HttpCmdResponse execute(HttpCmdRequest request) throws Exception {
		String jobDependencyJSON = request.getParam("jobDependency");
		if (StringUtils.isEmpty(jobDependencyJSON)) {
			return HttpCmdResponse.newResponse(true, "jobDependency should not be empty");
		}

		JobDependency jobDependency = JSON.parse(jobDependencyJSON, JobDependency.class);
		if (jobDependency == null) {
			return HttpCmdResponse.newResponse(true, "jobDependency should not be empty");
		}

		jobDependency.checkField();

		JobDependencyPo jobDependencyPo = new JobDependencyPo();
		jobDependencyPo.setJobId(jobDependency.getJobId());
		jobDependencyPo.setParentJobId(jobDependency.getParentJobId());
		jobDependencyPo.setTaskTrackerNodeGroup(jobDependency.getTaskTrackerNodeGroup());

		appContext.getExecutableJobDependency().add(jobDependencyPo);
		LOGGER.info("add job dependency succeed, {}", jobDependency);

		return HttpCmdResponse.newResponse(true, "add job dependency succeed");
	}
}
