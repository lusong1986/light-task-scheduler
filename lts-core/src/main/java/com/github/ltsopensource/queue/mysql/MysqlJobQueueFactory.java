package com.github.ltsopensource.queue.mysql;

import com.github.ltsopensource.core.AppContext;
import com.github.ltsopensource.core.cluster.Config;
import com.github.ltsopensource.queue.CronJobQueue;
import com.github.ltsopensource.queue.ExecutableJobDependency;
import com.github.ltsopensource.queue.ExecutableJobQueue;
import com.github.ltsopensource.queue.ExecutingJobQueue;
import com.github.ltsopensource.queue.JobFeedbackQueue;
import com.github.ltsopensource.queue.JobGrayFlag;
import com.github.ltsopensource.queue.JobQueueFactory;
import com.github.ltsopensource.queue.NodeGroupStore;
import com.github.ltsopensource.queue.PreLoader;
import com.github.ltsopensource.queue.RepeatJobQueue;
import com.github.ltsopensource.queue.SuspendJobQueue;

/**
 * @author Robert HG (254963746@qq.com) on 3/12/16.
 */
public class MysqlJobQueueFactory implements JobQueueFactory {

	@Override
	public CronJobQueue getCronJobQueue(Config config) {
		return new MysqlCronJobQueue(config);
	}

	@Override
	public RepeatJobQueue getRepeatJobQueue(Config config) {
		return new MysqlRepeatJobQueue(config);
	}

	@Override
	public ExecutableJobQueue getExecutableJobQueue(Config config) {
		return new MysqlExecutableJobQueue(config);
	}

	@Override
	public ExecutingJobQueue getExecutingJobQueue(Config config) {
		return new MysqlExecutingJobQueue(config);
	}

	@Override
	public JobFeedbackQueue getJobFeedbackQueue(Config config) {
		return new MysqlJobFeedbackQueue(config);
	}

	@Override
	public NodeGroupStore getNodeGroupStore(Config config) {
		return new MysqlNodeGroupStore(config);
	}

	@Override
	public SuspendJobQueue getSuspendJobQueue(Config config) {
		return new MysqlSuspendJobQueue(config);
	}

	@Override
	public PreLoader getPreLoader(AppContext appContext) {
		return new MysqlPreLoader(appContext);
	}

	@Override
	public JobGrayFlag getJobGrayFlag(Config config) {
		return new MysqlJobGrayFlag(config);
	}

	@Override
	public ExecutableJobDependency getExecutableJobDependency(Config config) {
		return new MysqlExecutableJobDependency(config);
	}
}
