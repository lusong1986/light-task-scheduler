package com.github.ltsopensource.jobtracker;

import com.github.ltsopensource.biz.logger.SmartJobLogger;
import com.github.ltsopensource.core.cluster.AbstractServerNode;
import com.github.ltsopensource.core.spi.ServiceLoader;
import com.github.ltsopensource.jobtracker.channel.ChannelManager;
import com.github.ltsopensource.jobtracker.cmd.AddJobDependencyHttpCmd;
import com.github.ltsopensource.jobtracker.cmd.AddJobGrayCommand;
import com.github.ltsopensource.jobtracker.cmd.AddJobHttpCmd;
import com.github.ltsopensource.jobtracker.cmd.DelJobGrayCommand;
import com.github.ltsopensource.jobtracker.cmd.GetProcessorThreadInfoHttpCmd;
import com.github.ltsopensource.jobtracker.cmd.GetTaskTrackerNodeHttpCmd;
import com.github.ltsopensource.jobtracker.cmd.LoadJobHttpCmd;
import com.github.ltsopensource.jobtracker.cmd.RecoveryAllJobCommand;
import com.github.ltsopensource.jobtracker.cmd.SuspendAllJobCommand;
import com.github.ltsopensource.jobtracker.cmd.TriggerJobManuallyHttpCmd;
import com.github.ltsopensource.jobtracker.cmd.UpdateJobGrayCommand;
import com.github.ltsopensource.jobtracker.domain.JobTrackerAppContext;
import com.github.ltsopensource.jobtracker.domain.JobTrackerNode;
import com.github.ltsopensource.jobtracker.monitor.JobTrackerMStatReporter;
import com.github.ltsopensource.jobtracker.processor.RemotingDispatcher;
import com.github.ltsopensource.jobtracker.sender.JobSender;
import com.github.ltsopensource.jobtracker.support.JobReceiver;
import com.github.ltsopensource.jobtracker.support.NonRelyOnPrevCycleJobScheduler;
import com.github.ltsopensource.jobtracker.support.OldDataHandler;
import com.github.ltsopensource.jobtracker.support.checker.ExecutableDeadJobChecker;
import com.github.ltsopensource.jobtracker.support.checker.ExecutingDeadJobChecker;
import com.github.ltsopensource.jobtracker.support.checker.FeedbackJobSendChecker;
import com.github.ltsopensource.jobtracker.support.cluster.JobClientManager;
import com.github.ltsopensource.jobtracker.support.cluster.TaskTrackerManager;
import com.github.ltsopensource.jobtracker.support.listener.JobNodeChangeListener;
import com.github.ltsopensource.jobtracker.support.listener.JobTrackerMasterChangeListener;
import com.github.ltsopensource.jobtracker.support.policy.OldDataDeletePolicy;
import com.github.ltsopensource.queue.JobQueueFactory;
import com.github.ltsopensource.remoting.RemotingProcessor;

/**
 * @author Robert HG (254963746@qq.com) on 7/23/14.
 */
public class JobTracker extends AbstractServerNode<JobTrackerNode, JobTrackerAppContext> {

	public JobTracker() {
		// 添加节点变化监听器
		addNodeChangeListener(new JobNodeChangeListener(appContext));
		// 添加master节点变化监听器
		addMasterChangeListener(new JobTrackerMasterChangeListener(appContext));
	}

	@Override
	protected void beforeStart() {
		// 监控中心
		appContext.setMStatReporter(new JobTrackerMStatReporter(appContext));
		// channel 管理者
		appContext.setChannelManager(new ChannelManager());
		// JobClient 管理者
		appContext.setJobClientManager(new JobClientManager(appContext));
		// TaskTracker 管理者
		appContext.setTaskTrackerManager(new TaskTrackerManager(appContext));

		// injectRemotingServer
		appContext.setRemotingServer(remotingServer);
		appContext.setJobLogger(new SmartJobLogger(appContext));

		JobQueueFactory factory = ServiceLoader.load(JobQueueFactory.class, config);

		appContext.setExecutableJobQueue(factory.getExecutableJobQueue(config));
		appContext.setExecutingJobQueue(factory.getExecutingJobQueue(config));
		appContext.setCronJobQueue(factory.getCronJobQueue(config));
		appContext.setRepeatJobQueue(factory.getRepeatJobQueue(config));
		appContext.setSuspendJobQueue(factory.getSuspendJobQueue(config));
		appContext.setJobFeedbackQueue(factory.getJobFeedbackQueue(config));
		appContext.setNodeGroupStore(factory.getNodeGroupStore(config));
		appContext.setPreLoader(factory.getPreLoader(appContext));
		appContext.setJobGrayFlag(factory.getJobGrayFlag(config));
		appContext.setExecutableJobDependency(factory.getExecutableJobDependency(config));
		appContext.setJobReceiver(new JobReceiver(appContext));
		appContext.setJobSender(new JobSender(appContext));
		appContext.setNonRelyOnPrevCycleJobScheduler(new NonRelyOnPrevCycleJobScheduler(appContext));
		appContext.setExecutableDeadJobChecker(new ExecutableDeadJobChecker(appContext));
		appContext.setExecutingDeadJobChecker(new ExecutingDeadJobChecker(appContext));
		appContext.setFeedbackJobSendChecker(new FeedbackJobSendChecker(appContext));

		appContext.getHttpCmdServer().registerCommands(
				new LoadJobHttpCmd(appContext), // 手动加载任务
				new AddJobHttpCmd(appContext),// 添加任务
				new TriggerJobManuallyHttpCmd(appContext), new AddJobGrayCommand(appContext),
				new DelJobGrayCommand(appContext), new UpdateJobGrayCommand(appContext),
				new AddJobDependencyHttpCmd(appContext), new SuspendAllJobCommand(appContext),
				new RecoveryAllJobCommand(appContext), new GetTaskTrackerNodeHttpCmd(appContext),
				new GetProcessorThreadInfoHttpCmd(appContext));

		if (appContext.getOldDataHandler() == null) {
			appContext.setOldDataHandler(new OldDataDeletePolicy());
		}
	}

	@Override
	protected void afterStart() {
		appContext.getChannelManager().start();
		appContext.getMStatReporter().start();
	}

	@Override
	protected void afterStop() {
		appContext.getChannelManager().stop();
		appContext.getMStatReporter().stop();
		appContext.getHttpCmdServer().stop();
	}

	@Override
	protected void beforeStop() {
	}

	@Override
	protected RemotingProcessor getDefaultProcessor() {
		return new RemotingDispatcher(appContext);
	}

	public void setOldDataHandler(OldDataHandler oldDataHandler) {
		appContext.setOldDataHandler(oldDataHandler);
	}

}
