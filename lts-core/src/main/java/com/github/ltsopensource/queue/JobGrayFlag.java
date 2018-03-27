package com.github.ltsopensource.queue;

import com.github.ltsopensource.admin.request.JobQueueReq;
import com.github.ltsopensource.admin.response.PaginationRsp;
import com.github.ltsopensource.queue.domain.JobGrayPo;

public interface JobGrayFlag {

	void add(String taskId, String gray);

	boolean remove(String taskId);

	boolean removeById(String id);

	void update(String taskId, String gray);

	PaginationRsp<JobGrayPo> pageSelect(JobQueueReq request);
}
