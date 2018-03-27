package com.github.ltsopensource.core.protocol.command;

import java.util.List;

import com.github.ltsopensource.core.domain.JobMeta;
import com.github.ltsopensource.remoting.annotation.NotNull;

/**
 * @author Robert HG (254963746@qq.com) on 8/14/14.
 */
public class JobPushRequest extends AbstractRemotingCommandBody {

	private static final long serialVersionUID = 2986743693237022215L;

	@NotNull
	private List<JobMeta> jobMetaList;

	public List<JobMeta> getJobMetaList() {
		return jobMetaList;
	}

	public void setJobMetaList(List<JobMeta> jobMetaList) {
		this.jobMetaList = jobMetaList;
	}
}
