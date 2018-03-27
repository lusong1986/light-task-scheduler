package com.github.ltsopensource.jobtracker.cmd;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import com.github.ltsopensource.admin.request.JobQueueReq;
import com.github.ltsopensource.admin.response.PaginationRsp;
import com.github.ltsopensource.biz.logger.JobLogUtils;
import com.github.ltsopensource.biz.logger.domain.LogType;
import com.github.ltsopensource.cmd.HttpCmdProc;
import com.github.ltsopensource.cmd.HttpCmdRequest;
import com.github.ltsopensource.cmd.HttpCmdResponse;
import com.github.ltsopensource.core.cmd.HttpCmdNames;
import com.github.ltsopensource.core.commons.utils.CatUtils;
import com.github.ltsopensource.core.constant.Constants;
import com.github.ltsopensource.core.constant.ExtConfig;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.core.support.CronExpressionUtils;
import com.github.ltsopensource.core.support.JobUtils;
import com.github.ltsopensource.core.support.SystemClock;
import com.github.ltsopensource.jobtracker.domain.JobTrackerAppContext;
import com.github.ltsopensource.queue.domain.JobPo;
import com.github.ltsopensource.queue.support.NonRelyJobUtils;
import com.github.ltsopensource.store.jdbc.exception.DupEntryException;

/**
 * http://*:8719/JT_192.168.0.101/recovery_all_job_cmd
 * 
 * @author lusong
 *
 */
public class RecoveryAllJobCommand implements HttpCmdProc {

	private final Logger LOGGER = LoggerFactory.getLogger(RecoveryAllJobCommand.class);

	private JobTrackerAppContext appContext;
	private int scheduleIntervalMinute;

	public RecoveryAllJobCommand(JobTrackerAppContext appContext) {
		this.appContext = appContext;
		this.scheduleIntervalMinute = this.appContext.getConfig().getParameter(
				ExtConfig.JOB_TRACKER_NON_RELYON_PREV_CYCLE_JOB_SCHEDULER_INTERVAL_MINUTE, 10);
	}

	@Override
	public String nodeIdentity() {
		return appContext.getConfig().getIdentity();
	}

	@Override
	public String getCommand() {
		return HttpCmdNames.HTTP_CMD_RECOVERY_ALL_JOB;
	}

	@Override
	public HttpCmdResponse execute(HttpCmdRequest request) throws Exception {

		final int limit = 100;
		final List<JobPo> suspendJobs = new LinkedList<JobPo>();
		for (int i = 1;; i++) {
			int start = (i - 1) * limit;
			JobQueueReq req = new JobQueueReq();
			req.setStart(start);
			req.setLimit(limit);
			PaginationRsp<JobPo> paginationRsp = appContext.getSuspendJobQueue().pageSelect(req);
			if (paginationRsp.getRows().size() <= 0) {
				break;
			} else {
				suspendJobs.addAll(paginationRsp.getRows());
			}
		}

		LOGGER.info("start recovery all suspend jobs");
		for (JobPo jobPo : suspendJobs) {
			String ret = suspendJobRecovery(jobPo.getJobId());
			LOGGER.info(jobPo.getTaskId() + " recovery result :" + ret);
		}
		LOGGER.info("end recovery all suspend jobs");

		return HttpCmdResponse.newResponse(true, "add job gray succeed");
	}

	private String suspendJobRecovery(final String jobId) {
		JobPo jobPo = appContext.getSuspendJobQueue().getJob(jobId);
		if (jobPo == null) {
			return "任务不存在，或者已经删除";
		}

		// 判断是Cron任务还是Repeat任务
		if (jobPo.isCron()) {
			// 先恢复,才能删除
			Date nextTriggerTime = CronExpressionUtils.getNextTriggerTime(jobPo.getCronExpression());
			if (nextTriggerTime != null) {
				jobPo.setGmtModified(SystemClock.now());
				try {
					// 1.add to cron job queue
					appContext.getCronJobQueue().add(jobPo);
				} catch (DupEntryException e) {
					LOGGER.error(e.getMessage(), e);
					CatUtils.recordEvent(false, jobPo.getTaskId(), "RESUME_SUSPEND_JOB");
					return "Cron队列中任务已经存在，请检查";
				} catch (Exception e) {
					LOGGER.error(e.getMessage(), e);
					CatUtils.recordEvent(false, jobPo.getTaskId(), "RESUME_SUSPEND_JOB");
					return "插入Cron队列中任务错误, error:" + e.getMessage();
				}

				if (jobPo.getRelyOnPrevCycle()) {
					try {
						// 2. add to executable queue
						jobPo.setTriggerTime(nextTriggerTime.getTime());
						jobPo.setInternalExtParam(Constants.EXE_SEQ_ID, JobUtils.generateExeSeqId(jobPo));
						appContext.getExecutableJobQueue().add(jobPo);
					} catch (DupEntryException e) {
						LOGGER.error(e.getMessage(), e);
						CatUtils.recordEvent(false, jobPo.getTaskId(), "RESUME_SUSPEND_JOB");
						return "等待执行队列中任务已经存在，请检查";
					} catch (Exception e) {
						LOGGER.error(e.getMessage(), e);
						CatUtils.recordEvent(false, jobPo.getTaskId(), "RESUME_SUSPEND_JOB");
						return "插入等待执行队列中任务错误, error:" + e.getMessage();
					}
				} else {
					// 不依赖上一周期的
					generateCronJobForInterval(jobPo, new Date(SystemClock.now()));
				}

			} else {
				CatUtils.recordEvent(false, jobPo.getTaskId(), "RESUME_SUSPEND_JOB");
				return "该任务已经无效, 或者已经没有下一轮执行时间点, 请直接删除";
			}
		} else if (jobPo.isRepeatable()) {
			// 先恢复,才能删除
			if (jobPo.getRepeatCount() == -1 || jobPo.getRepeatedCount() < jobPo.getRepeatCount()) {
				jobPo.setGmtModified(SystemClock.now());
				try {
					// 1.add to repeat job queue
					appContext.getRepeatJobQueue().add(jobPo);
				} catch (DupEntryException e) {
					LOGGER.error(e.getMessage(), e);
					CatUtils.recordEvent(false, jobPo.getTaskId(), "RESUME_SUSPEND_JOB");
					return "Repeat队列中任务已经存在，请检查";
				} catch (Exception e) {
					LOGGER.error(e.getMessage(), e);
					CatUtils.recordEvent(false, jobPo.getTaskId(), "RESUME_SUSPEND_JOB");
					return "插入Repeat队列中任务错误, error:" + e.getMessage();
				}

				if (jobPo.getRelyOnPrevCycle()) {
					try {
						// 2. add to executable queue
						JobPo repeatJob = appContext.getRepeatJobQueue().getJob(jobId);
						long nextTriggerTime = JobUtils.getRepeatNextTriggerTime(repeatJob);
						jobPo.setTriggerTime(nextTriggerTime);
						jobPo.setInternalExtParam(Constants.EXE_SEQ_ID, JobUtils.generateExeSeqId(jobPo));
						appContext.getExecutableJobQueue().add(jobPo);
					} catch (DupEntryException e) {
						LOGGER.error(e.getMessage(), e);
						CatUtils.recordEvent(false, jobPo.getTaskId(), "RESUME_SUSPEND_JOB");
						return "等待执行队列中任务已经存在，请检查";
					} catch (Exception e) {
						LOGGER.error(e.getMessage(), e);
						CatUtils.recordEvent(false, jobPo.getTaskId(), "RESUME_SUSPEND_JOB");
						return "插入等待执行队列中任务错误, error:" + e.getMessage();
					}
				} else {
					// 不依赖上一周期的
					generateRepeatJobForInterval(jobPo, new Date(SystemClock.now()));
				}
			} else {
				CatUtils.recordEvent(false, jobPo.getTaskId(), "RESUME_SUSPEND_JOB");
				return "该任务已经无效, 或者已经没有下一轮执行时间点, 请直接删除";
			}
		}

		// 从暂停表中移除
		if (!appContext.getSuspendJobQueue().remove(jobId)) {
			CatUtils.recordEvent(false, jobPo.getTaskId(), "RESUME_SUSPEND_JOB");
			return "恢复暂停任务失败，请重试";
		}

		CatUtils.recordEvent(true, jobPo.getTaskId(), "RESUME_SUSPEND_JOB");
		JobLogUtils.log(LogType.RESUME, jobPo, appContext.getJobLogger());
		return "ok";
	}

	private void generateCronJobForInterval(final JobPo jobPo, Date lastGenerateTime) {
		NonRelyJobUtils.addCronJobForInterval(appContext.getExecutableJobQueue(), appContext.getCronJobQueue(),
				scheduleIntervalMinute, jobPo, lastGenerateTime);
	}

	private void generateRepeatJobForInterval(final JobPo jobPo, Date lastGenerateTime) {
		NonRelyJobUtils.addRepeatJobForInterval(appContext.getExecutableJobQueue(), appContext.getRepeatJobQueue(),
				scheduleIntervalMinute, jobPo, lastGenerateTime);
	}

}
