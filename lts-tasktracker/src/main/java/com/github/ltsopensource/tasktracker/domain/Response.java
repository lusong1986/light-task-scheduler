package com.github.ltsopensource.tasktracker.domain;

import com.github.ltsopensource.core.domain.Action;
import com.github.ltsopensource.core.domain.JobMeta;

/**
 * @author Robert HG (254963746@qq.com) on 8/14/14.
 */
public class Response {

	private static final int MSG_MAX_LENGTH = 10 * 1024;

	private Action action;

	private String msg;

	private JobMeta jobMeta;

	/**
	 * 是否接收新任务
	 */
	private boolean receiveNewJob = true;

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		if (msg != null && msg.length() > MSG_MAX_LENGTH) {
			this.msg = msg.substring(0, MSG_MAX_LENGTH);
		} else {
			this.msg = msg;
		}
	}

	public JobMeta getJobMeta() {
		return jobMeta;
	}

	public void setJobMeta(JobMeta jobMeta) {
		this.jobMeta = jobMeta;
	}

	public boolean isReceiveNewJob() {
		return receiveNewJob;
	}

	public void setReceiveNewJob(boolean receiveNewJob) {
		this.receiveNewJob = receiveNewJob;
	}

	public Action getAction() {
		return action;
	}

	public void setAction(Action action) {
		this.action = action;
	}
}
