package com.github.ltsopensource.queue;

import java.util.List;

import com.github.ltsopensource.queue.domain.JobPo;

/**
 * @author Robert HG (254963746@qq.com) on 4/4/16.
 */
public interface SchedulerJobQueue extends JobQueue {
	/**
	 * 更新 lastGenerateTriggerTime
	 */
	boolean updateLastGenerateTriggerTime(String jobId, Long lastGenerateTriggerTime);

	List<JobPo> getNeedGenerateJobPos(Long checkTime, int topSize);
}
