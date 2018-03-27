package com.github.ltsopensource.jobtracker.complete;

import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.alibaba.fastjson.JSONObject;
import com.dianping.cat.message.Transaction;
import com.github.ltsopensource.admin.request.JobQueueReq;
import com.github.ltsopensource.admin.response.PaginationRsp;
import com.github.ltsopensource.biz.logger.domain.JobLogPo;
import com.github.ltsopensource.biz.logger.domain.LogType;
import com.github.ltsopensource.core.commons.utils.CatUtils;
import com.github.ltsopensource.core.commons.utils.CollectionUtils;
import com.github.ltsopensource.core.constant.Constants;
import com.github.ltsopensource.core.constant.Level;
import com.github.ltsopensource.core.domain.Action;
import com.github.ltsopensource.core.domain.JobMeta;
import com.github.ltsopensource.core.domain.JobRunResult;
import com.github.ltsopensource.core.factory.NamedThreadFactory;
import com.github.ltsopensource.core.json.JSON;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.core.support.CronExpressionUtils;
import com.github.ltsopensource.core.support.JobDomainConverter;
import com.github.ltsopensource.core.support.JobUtils;
import com.github.ltsopensource.core.support.SystemClock;
import com.github.ltsopensource.jobtracker.domain.JobTrackerAppContext;
import com.github.ltsopensource.jobtracker.support.JobTraceUtil;
import com.github.ltsopensource.queue.domain.JobDependencyPo;
import com.github.ltsopensource.queue.domain.JobPo;
import com.github.ltsopensource.store.jdbc.exception.DupEntryException;

/**
 * @author Robert HG (254963746@qq.com) on 11/11/15.
 */
public class JobFinishHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(JobFinishHandler.class);

	private JobTrackerAppContext appContext;

	private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(15,
			new NamedThreadFactory(JobFinishHandler.class.getSimpleName() + "-ScheduledExecutor", true));

	public JobFinishHandler(JobTrackerAppContext appContext) {
		this.appContext = appContext;
	}

	public void onComplete(List<JobRunResult> results) {
		if (CollectionUtils.isEmpty(results)) {
			return;
		}

		final String taskId = results.get(0).getJobMeta().getRealTaskId();
		final Transaction transaction = CatUtils.catTransaction("JOB_FINISH_HANDLER", taskId);

		try {
			for (JobRunResult result : results) {
				JobMeta jobMeta = result.getJobMeta();

				final boolean isOnce = Boolean.TRUE.toString().equals(jobMeta.getInternalExtParam(Constants.ONCE));
				if (isOnce) {
					CatUtils.recordEvent(true, jobMeta.getRealTaskId(), "ONCEJOB");
				}

				if (jobMeta.getJob().isCron()) {
					// 是 Cron任务
					if (isOnce) {
						finishNoReplyPrevCronJob(jobMeta);
					} else {
						finishCronJob(jobMeta.getJobId());
					}
				} else if (jobMeta.getJob().isRepeatable()) {
					// 当前完成的job是否是重试的
					final boolean isRetryForThisTime = Boolean.TRUE.toString().equals(
							jobMeta.getInternalExtParam(Constants.IS_RETRY_JOB));
					if (isRetryForThisTime) {
						CatUtils.recordEvent(true, jobMeta.getRealTaskId(), "REPEAT_RETRY_JOB");
					}

					if (isOnce) {
						finishNoReplyPrevRepeatJob(jobMeta, isRetryForThisTime);
					} else {
						finishRepeatJob(jobMeta.getJobId(), isRetryForThisTime);
					}
				}

				// 从正在执行的队列中移除
				appContext.getExecutingJobQueue().remove(jobMeta.getJobId());

				if (result.getAction().equals(Action.EXECUTE_SUCCESS)) {
					// 使这个job的子job有执行权限
					appContext.getExecutableJobDependency().updateCanExecuteByParentJob(jobMeta.getJobId(),
							jobMeta.getJob().getTaskTrackerNodeGroup());
				} else {
					// 使这个job的子job没有执行权限，并且更新子job的triggertime
					final String parentJobId = jobMeta.getJobId();
					final String parentJobNodeGroup = jobMeta.getJob().getTaskTrackerNodeGroup();
					appContext.getExecutableJobDependency().updateCanNotExecuteByParentJob(parentJobId,
							parentJobNodeGroup);

					updateSonJobTriggerTime(parentJobId, parentJobNodeGroup, null);
				}

				if (jobMeta.getJob().isCron() || jobMeta.getJob().isRepeatable()) {
					// 使这个循环子job又变成不可执行状态，等待父job来更新它
					appContext.getExecutableJobDependency().updateCanNotExecuteBySonJob(jobMeta.getJobId());
				} else {
					// 非cron，非repeat任务
					appContext.getJobGrayFlag().remove(jobMeta.getJob().getTaskId());
					appContext.getExecutableJobDependency().remove(jobMeta.getJobId());
				}
			}
			CatUtils.catSuccess(transaction);
		} finally {
			CatUtils.catComplete(transaction);
		}
	}

	/**
	 * 递归更新cron 子job的triggertime
	 * 
	 * @param parentJobId
	 * @param parentJobNodeGroup
	 * @param parentJobNextTriggerTime
	 */
	private void updateSonJobTriggerTime(final String parentJobId, final String parentJobNodeGroup,
			Date parentJobNextTriggerTime) {
		try {
			final JobPo parentJobPo = appContext.getCronJobQueue().getJob(parentJobId);
			if (null == parentJobPo) {
				return;
			}

			if (null == parentJobNextTriggerTime) {
				parentJobNextTriggerTime = CronExpressionUtils.getNextTriggerTime(parentJobPo.getCronExpression());
			}

			JobQueueReq depQueueReq = new JobQueueReq();
			depQueueReq.setParentJobId(parentJobId);
			depQueueReq.setTaskTrackerNodeGroup(parentJobNodeGroup);
			PaginationRsp<JobDependencyPo> depPagRsp = appContext.getExecutableJobDependency().pageSelect(depQueueReq);
			if (depPagRsp != null && depPagRsp.getRows() != null && depPagRsp.getRows().size() > 0) {
				for (JobDependencyPo jobDependencyPo : depPagRsp.getRows()) {
					final String sonJobId = jobDependencyPo.getJobId();
					final JobPo cronJobPo = appContext.getCronJobQueue().getJob(sonJobId);
					if (null == cronJobPo) {
						continue;
					}

					JobQueueReq queryRequest = new JobQueueReq();
					queryRequest.setJobId(sonJobId);
					queryRequest.setTaskTrackerNodeGroup(cronJobPo.getTaskTrackerNodeGroup());
					PaginationRsp<JobPo> executableJobs = appContext.getExecutableJobQueue().pageSelect(queryRequest);
					if (executableJobs != null && executableJobs.getRows() != null
							&& executableJobs.getRows().size() > 0) {
						final JobPo executableSonJob = executableJobs.getRows().get(0);
						JobQueueReq updateRequest = new JobQueueReq();
						updateRequest.setJobId(executableSonJob.getJobId());
						updateRequest.setTaskTrackerNodeGroup(executableSonJob.getTaskTrackerNodeGroup());

						// 下次parent job之后
						final Date sonJobNewTriggerTime = CronExpressionUtils.getNextTriggerTime(
								executableSonJob.getCronExpression(), parentJobNextTriggerTime);
						LOGGER.info("update son job [" + executableSonJob.getRealTaskId() + "] 's triggerTime to :"
								+ sonJobNewTriggerTime.toString());
						updateRequest.setTriggerTime(sonJobNewTriggerTime);
						appContext.getExecutableJobQueue().selectiveUpdateByJobId(updateRequest);
						CatUtils.recordEvent(true, executableSonJob.getRealTaskId(), "UPDATE_SONJOB_TRIGGERTIME");

						// 继续更新它的子job的时间
						updateSonJobTriggerTime(executableSonJob.getJobId(),
								executableSonJob.getTaskTrackerNodeGroup(), sonJobNewTriggerTime);
					}
				}
			}
		} catch (Exception e) {
			LOGGER.warn("updateSonJobTriggerTime parentJobId:" + parentJobId, e);
		}
	}

	private void finishCronJob(String jobId) {
		final JobPo jobPo = appContext.getCronJobQueue().getJob(jobId);
		if (jobPo == null) {
			// 可能任务队列中改条记录被删除了
			return;
		}

		final Date nextTriggerTime = CronExpressionUtils.getNextTriggerTime(jobPo.getCronExpression());
		if (nextTriggerTime == null) {
			// 从CronJob队列中移除
			appContext.getCronJobQueue().remove(jobId);
			appContext.getJobGrayFlag().remove(jobPo.getTaskId());
			appContext.getExecutableJobDependency().remove(jobId);
			appContext.getExecutableJobDependency().removeDependency(jobId, jobPo.getTaskTrackerNodeGroup());
			jobRemoveLog(jobPo, "Cron");
			return;
		}

		final Transaction transaction = CatUtils.catTransaction("FINISH_CRON_JOB", jobPo.getRealTaskId());

		// 表示下次还要执行
		try {
			jobPo.setTaskTrackerIdentity(null);
			jobPo.setIsRunning(false);

			final long nextTriggerTimeInMills = nextTriggerTime.getTime();
			jobPo.setTriggerTime(nextTriggerTimeInMills);

			final long gmtModified = SystemClock.now();
			jobPo.setGmtModified(gmtModified);
			jobPo.setInternalExtParam(Constants.EXE_SEQ_ID, JobUtils.generateExeSeqId(jobPo));
			JobTraceUtil.setNewCatMessageIds(jobPo);

			appContext.getExecutableJobQueue().add(jobPo);

			final JSONObject jobJson = new JSONObject();
			jobJson.put("jobId", jobPo.getJobId());
			jobJson.put("realTaskId", jobPo.getRealTaskId());
			jobJson.put("taskTrackerIdentity", jobPo.getTaskTrackerIdentity());
			jobJson.put("taskTrackerNodeGroup", jobPo.getTaskTrackerNodeGroup());
			jobJson.put("triggerTime", jobPo.getTriggerTime());
			transaction.addData("NEXT_JOB", jobJson.toJSONString());

			// job执行30秒后检查 job是否按时执行了
			final long delayTimeInMills = nextTriggerTimeInMills - gmtModified + 30000;
			final Runnable checkJobRunnable = new Runnable() {

				@Override
				public void run() {
					try {
						final JobQueueReq request = new JobQueueReq();
						request.setJobId(jobPo.getJobId());
						request.setTaskId(jobPo.getTaskId());
						request.setTaskTrackerNodeGroup(jobPo.getTaskTrackerNodeGroup());

						final Date modifiedDate = new Date(gmtModified);
						request.setStartGmtModified(modifiedDate);
						request.setEndGmtModified(modifiedDate);

						PaginationRsp<JobPo> executableJobs = appContext.getExecutableJobQueue().pageSelect(request);
						if (executableJobs != null && executableJobs.getResults() > 0) {
							LOGGER.warn("NO_EXECUTE_JOBS:" + executableJobs.getRows().get(0));
							CatUtils.recordEvent(true, jobPo.getRealTaskId(), "NOEXECUTE");
						}
					} catch (Throwable e) {
						LOGGER.warn("check no execute cron jobs warn:" + e.getMessage());
					}
				}
			};
			scheduledExecutorService.schedule(checkJobRunnable, delayTimeInMills, TimeUnit.MILLISECONDS);

			CatUtils.catSuccess(transaction);
		} catch (DupEntryException e) {
			CatUtils.recordEvent(false, jobPo.getRealTaskId(), "ADDCRON_EXECUTABLE_FAILED");
			LOGGER.warn("ExecutableJobQueue already exist:" + JSON.toJSONString(jobPo), e);
		} finally {
			CatUtils.catComplete(transaction);
		}
	}

	private void finishNoReplyPrevCronJob(JobMeta jobMeta) {
		JobPo jobPo = appContext.getCronJobQueue().getJob(jobMeta.getJob().getTaskTrackerNodeGroup(),
				jobMeta.getRealTaskId());
		if (jobPo == null) {
			// 可能任务队列中改条记录被删除了
			return;
		}

		Date nextTriggerTime = CronExpressionUtils.getNextTriggerTime(jobPo.getCronExpression());
		if (nextTriggerTime == null) {
			// 检查可执行队列中是否还有
			if (appContext.getExecutableJobQueue().countJob(jobPo.getRealTaskId(), jobPo.getTaskTrackerNodeGroup()) == 0) {
				// TODO 检查执行中队列是否还有
				// 从CronJob队列中移除
				appContext.getCronJobQueue().remove(jobPo.getJobId());
				appContext.getJobGrayFlag().remove(jobPo.getTaskId());
				appContext.getExecutableJobDependency().remove(jobPo.getJobId());
				appContext.getExecutableJobDependency().removeDependency(jobPo.getJobId(),
						jobPo.getTaskTrackerNodeGroup());
				jobRemoveLog(jobPo, "Cron");
			}
		}
	}

	private void finishNoReplyPrevRepeatJob(JobMeta jobMeta, boolean isRetryForThisTime) {
		JobPo jobPo = appContext.getRepeatJobQueue().getJob(jobMeta.getJob().getTaskTrackerNodeGroup(),
				jobMeta.getRealTaskId());
		if (jobPo == null) {
			// 可能任务队列中改条记录被删除了
			return;
		}

		if (jobPo.getRepeatCount() != -1 && (jobPo.getRepeatedCount() + 1) >= jobPo.getRepeatCount()) {
			CatUtils.recordEvent(true, jobPo.getRealTaskId(), "REMOVE_FINISHED_REPEAT_JOB");
			// 已经重试完成, 那么删除, 这里可以不用check可执行队列是否还有,因为这里依赖的是计数
			appContext.getRepeatJobQueue().remove(jobPo.getJobId());
			appContext.getJobGrayFlag().remove(jobPo.getTaskId());
			appContext.getExecutableJobDependency().remove(jobPo.getJobId());
			appContext.getExecutableJobDependency().removeDependency(jobPo.getJobId(), jobPo.getTaskTrackerNodeGroup());
			jobRemoveLog(jobPo, "Repeat");
			return;
		}

		// 如果当前完成的job是重试的,那么不要增加repeatedCount
		if (!isRetryForThisTime) {
			// 更新repeatJob的重复次数
			final int jobQueueRepeatedCount = appContext.getRepeatJobQueue().incRepeatedCount(jobPo.getJobId());
			if (jobQueueRepeatedCount >= jobPo.getRepeatCount()) {
				CatUtils.recordEvent(true, jobPo.getRealTaskId(), "REMOVE_FINISHED_REPEAT_JOB");
				// 有可能前面没有被删除掉，这里再次做下判断
				appContext.getRepeatJobQueue().remove(jobPo.getJobId());
				appContext.getJobGrayFlag().remove(jobPo.getTaskId());
				appContext.getExecutableJobDependency().remove(jobPo.getJobId());
				appContext.getExecutableJobDependency().removeDependency(jobPo.getJobId(),
						jobPo.getTaskTrackerNodeGroup());
				jobRemoveLog(jobPo, "Repeat");
			}
		}
	}

	private void finishRepeatJob(String jobId, boolean isRetryForThisTime) {
		final JobPo jobPo = appContext.getRepeatJobQueue().getJob(jobId);
		if (jobPo == null) {
			// 可能任务队列中改条记录被删除了
			return;
		}

		final Transaction transaction = CatUtils.catTransaction("FINISH_REPEAT_JOB", jobPo.getRealTaskId());

		if (jobPo.getRepeatCount() != -1 && (jobPo.getRepeatedCount() + 1) >= jobPo.getRepeatCount()) {
			// 已经重试完成, 那么删除
			CatUtils.recordEvent(true, jobPo.getRealTaskId(), "REMOVE_FINISHED_REPEAT_JOB");
			appContext.getRepeatJobQueue().remove(jobId);
			appContext.getJobGrayFlag().remove(jobPo.getTaskId());
			appContext.getExecutableJobDependency().remove(jobId);
			appContext.getExecutableJobDependency().removeDependency(jobId, jobPo.getTaskTrackerNodeGroup());
			jobRemoveLog(jobPo, "Repeat");

			CatUtils.catSuccess(transaction);
			CatUtils.catComplete(transaction);
			return;
		}

		int repeatedCount = jobPo.getRepeatedCount();
		// 如果当前完成的job是重试的,那么不要增加repeatedCount
		if (!isRetryForThisTime) {
			// 更新repeatJob的重复次数
			repeatedCount = appContext.getRepeatJobQueue().incRepeatedCount(jobId);
			if (repeatedCount >= jobPo.getRepeatCount()) {
				CatUtils.recordEvent(true, jobPo.getRealTaskId(), "REMOVE_FINISHED_REPEAT_JOB");
				// 有可能前面没有被删除掉，这里再次做下判断
				appContext.getRepeatJobQueue().remove(jobPo.getJobId());
				appContext.getJobGrayFlag().remove(jobPo.getTaskId());
				appContext.getExecutableJobDependency().remove(jobPo.getJobId());
				appContext.getExecutableJobDependency().removeDependency(jobPo.getJobId(),
						jobPo.getTaskTrackerNodeGroup());
				jobRemoveLog(jobPo, "Repeat");
			}
		}

		if (repeatedCount == -1) {
			// 表示任务已经被删除了
			return;
		}

		final long nexTriggerTimeInMills = JobUtils.getRepeatNextTriggerTime(jobPo);
		try {
			jobPo.setRepeatedCount(repeatedCount + 1);
			jobPo.setTaskTrackerIdentity(null);
			jobPo.setIsRunning(false);
			jobPo.setTriggerTime(nexTriggerTimeInMills);

			final long gmtModified = SystemClock.now();
			jobPo.setGmtModified(gmtModified);
			jobPo.setInternalExtParam(Constants.EXE_SEQ_ID, JobUtils.generateExeSeqId(jobPo));
			JobTraceUtil.setNewCatMessageIds(jobPo);

			appContext.getExecutableJobQueue().add(jobPo);

			final JSONObject jobJson = new JSONObject();
			jobJson.put("jobId", jobPo.getJobId());
			jobJson.put("realTaskId", jobPo.getRealTaskId());
			jobJson.put("taskTrackerIdentity", jobPo.getTaskTrackerIdentity());
			jobJson.put("taskTrackerNodeGroup", jobPo.getTaskTrackerNodeGroup());
			jobJson.put("triggerTime", jobPo.getTriggerTime());
			transaction.addData("NEXT_JOB", jobJson.toJSONString());

			// job执行30秒后检查 job是否按时执行了
			final long delayTimeInMills = nexTriggerTimeInMills - gmtModified + 30000;
			final Runnable checkJobRunnable = new Runnable() {

				@Override
				public void run() {
					try {
						final JobQueueReq request = new JobQueueReq();
						request.setJobId(jobPo.getJobId());
						request.setTaskId(jobPo.getTaskId());
						request.setTaskTrackerNodeGroup(jobPo.getTaskTrackerNodeGroup());

						final Date modifiedDate = new Date(gmtModified);
						request.setStartGmtModified(modifiedDate);
						request.setEndGmtModified(modifiedDate);

						PaginationRsp<JobPo> executableJobs = appContext.getExecutableJobQueue().pageSelect(request);
						if (executableJobs != null && executableJobs.getResults() > 0) {
							LOGGER.warn("NO_EXECUTE_JOBS:" + executableJobs.getRows().get(0));
							CatUtils.recordEvent(true, jobPo.getRealTaskId(), "NOEXECUTE");
						}
					} catch (Throwable e) {
						LOGGER.warn("check no execute repeat jobs warn:" + e.getMessage());
					}
				}
			};
			scheduledExecutorService.schedule(checkJobRunnable, delayTimeInMills, TimeUnit.MILLISECONDS);

			CatUtils.catSuccess(transaction);
		} catch (DupEntryException e) {
			CatUtils.recordEvent(false, jobPo.getRealTaskId(), "ADDREPEAT_EXECUTABLE_FAILED");
			LOGGER.warn("ExecutableJobQueue already exist:" + JSON.toJSONString(jobPo), e);
		} finally {
			CatUtils.catComplete(transaction);
		}
	}

	private void jobRemoveLog(JobPo jobPo, String type) {
		JobLogPo jobLogPo = JobDomainConverter.convertJobLog(jobPo);
		jobLogPo.setSuccess(true);
		jobLogPo.setLogType(LogType.DEL);
		jobLogPo.setLogTime(SystemClock.now());
		jobLogPo.setLevel(Level.INFO);
		jobLogPo.setMsg(type + " Job Finished");
		appContext.getJobLogger().log(jobLogPo);
	}
}
