package com.github.ltsopensource.core.domain;

import java.io.Serializable;

import com.github.ltsopensource.core.exception.JobSubmitException;

public class JobDependency implements Serializable {

	private static final long serialVersionUID = 1015363375788092120L;

	private String jobId;
	private String parentJobId;
	private String taskTrackerNodeGroup;

	public JobDependency() {
	}

	public JobDependency(String parentJobId, String taskTrackerNodeGroup) {
		this.parentJobId = parentJobId;
		this.taskTrackerNodeGroup = taskTrackerNodeGroup;
	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public String getTaskTrackerNodeGroup() {
		return taskTrackerNodeGroup;
	}

	public void setTaskTrackerNodeGroup(String taskTrackerNodeGroup) {
		this.taskTrackerNodeGroup = taskTrackerNodeGroup;
	}

	public String getParentJobId() {
		return parentJobId;
	}

	public void setParentJobId(String parentJobId) {
		this.parentJobId = parentJobId;
	}

	public void checkField() throws JobSubmitException {
		if (jobId == null) {
			throw new JobSubmitException("jobId can not be null! job is " + toString());
		}
		if (parentJobId == null) {
			throw new JobSubmitException("parentJobId can not be null!  job is " + toString());
		}
		if (taskTrackerNodeGroup == null) {
			throw new JobSubmitException("taskTrackerNodeGroup can not be null!  job is " + toString());
		}
	}
}
