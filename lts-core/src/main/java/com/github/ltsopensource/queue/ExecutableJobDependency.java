package com.github.ltsopensource.queue;

import com.github.ltsopensource.admin.request.JobQueueReq;
import com.github.ltsopensource.admin.response.PaginationRsp;
import com.github.ltsopensource.queue.domain.JobDependencyPo;

public interface ExecutableJobDependency {

	void add(String jobId, JobDependencyPo jobDependencyPo);

	void add(JobDependencyPo jobDependencyPo);

	void add(String jobId);

	boolean remove(String jobId);

	boolean removeById(String id);

	void removeDependency(String parentJobId, String taskTrackerNodeGroup);

	boolean removeDependency(String id);

	void updateCanExecuteByParentJob(String parentJobId, String taskTrackerNodeGroup);
	
	void updateCanNotExecuteByParentJob(String parentJobId, String taskTrackerNodeGroup);

	void updateCanNotExecuteBySonJob(String jobId);

	PaginationRsp<JobDependencyPo> pageSelect(JobQueueReq request);
}
