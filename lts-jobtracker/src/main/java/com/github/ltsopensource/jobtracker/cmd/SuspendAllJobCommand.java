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
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.core.support.SystemClock;
import com.github.ltsopensource.jobtracker.domain.JobTrackerAppContext;
import com.github.ltsopensource.queue.domain.JobPo;
import com.github.ltsopensource.store.jdbc.exception.DupEntryException;

/**
 * http://*:8719/JT_192.168.0.101/suspend_all_job_cmd
 * 
 * 只支持repeat和cron job
 * 
 * @author lusong
 *
 */
public class SuspendAllJobCommand implements HttpCmdProc {

	private final Logger LOGGER = LoggerFactory.getLogger(SuspendAllJobCommand.class);

	private JobTrackerAppContext appContext;

	public SuspendAllJobCommand(JobTrackerAppContext appContext) {
		this.appContext = appContext;
	}

	@Override
	public String nodeIdentity() {
		return appContext.getConfig().getIdentity();
	}

	@Override
	public String getCommand() {
		return HttpCmdNames.HTTP_CMD_SUSPEND_ALL_JOB;
	}

	@Override
	public HttpCmdResponse execute(HttpCmdRequest request) throws Exception {
		LOGGER.info("start execute suspend all jobs");

		final int limit = 100;
		final List<JobPo> cronJobs = new LinkedList<JobPo>();
		for (int i = 1;; i++) {
			int start = (i - 1) * limit;
			JobQueueReq req = new JobQueueReq();
			req.setStart(start);
			req.setLimit(limit);
			PaginationRsp<JobPo> paginationRsp = appContext.getCronJobQueue().pageSelect(req);
			if (paginationRsp.getRows().size() <= 0) {
				break;
			} else {
				LOGGER.info("add " + paginationRsp.getRows().size() + " cron jobs.");
				cronJobs.addAll(paginationRsp.getRows());
			}
		}

		LOGGER.info("start suspend all cron jobs:" + cronJobs.size());
		for (final JobPo jobPo : cronJobs) {
			String ret = cronJobSuspend(jobPo.getTaskTrackerNodeGroup(), jobPo.getJobId());
			LOGGER.info(jobPo.getTaskId() + " suspend result :" + ret);
		}
		LOGGER.info("end suspend all cron jobs");

		final List<JobPo> repeatJobs = new LinkedList<JobPo>();
		for (int i = 1;; i++) {
			int start = (i - 1) * limit;
			JobQueueReq req = new JobQueueReq();
			req.setStart(start);
			req.setLimit(limit);
			PaginationRsp<JobPo> paginationRsp = appContext.getRepeatJobQueue().pageSelect(req);
			if (paginationRsp.getRows().size() <= 0) {
				break;
			} else {
				repeatJobs.addAll(paginationRsp.getRows());
			}
		}

		LOGGER.info("start suspend all repeat jobs:" + repeatJobs.size());
		for (final JobPo jobPo : repeatJobs) {
			String ret = repeatJobSuspend(jobPo.getTaskTrackerNodeGroup(), jobPo.getJobId());
			LOGGER.info(jobPo.getTaskId() + " suspend result :" + ret);
		}
		LOGGER.info("end suspend all repeat jobs");

		return HttpCmdResponse.newResponse(true, "suspend all job succeed");
	}

	private String cronJobSuspend(final String taskTrackerNodeGroup, String jobId) {
		JobPo jobPo = appContext.getCronJobQueue().getJob(jobId);
		if (jobPo == null) {
			return "任务不存在，或者已经删除";
		}

		try {
			jobPo.setGmtModified(SystemClock.now());
			appContext.getSuspendJobQueue().add(jobPo);
		} catch (DupEntryException e) {
			LOGGER.error(e.getMessage(), e);
			return "该任务已经被暂停, 请检查暂停队列";
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			return "移动任务到暂停队列失败, error:" + e.getMessage();
		}

		try {
			appContext.getCronJobQueue().remove(jobId);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			return "删除Cron任务失败，请手动删除! error:" + e.getMessage();
		}

		try {
			if (!jobPo.getRelyOnPrevCycle()) {
				appContext.getCronJobQueue().updateLastGenerateTriggerTime(jobPo.getJobId(), new Date().getTime());
				appContext.getExecutableJobQueue().removeBatch(jobPo.getRealTaskId(), jobPo.getTaskTrackerNodeGroup());
			} else {
				appContext.getExecutableJobQueue().remove(taskTrackerNodeGroup, jobId);
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			return "删除等待执行的任务失败，请手动删除! error:" + e.getMessage();
		}

		CatUtils.recordEvent(true, jobPo.getTaskId(), "SUSPEND");
		// 记录日志
		JobLogUtils.log(LogType.SUSPEND, jobPo, appContext.getJobLogger());

		return "ok";
	}

	private String repeatJobSuspend(final String taskTrackerNodeGroup, final String jobId) {
		JobPo jobPo = appContext.getRepeatJobQueue().getJob(jobId);
		if (jobPo == null) {
			return "任务不存在，或者已经删除";
		}

		try {
			jobPo.setGmtModified(SystemClock.now());
			appContext.getSuspendJobQueue().add(jobPo);
		} catch (DupEntryException e) {
			LOGGER.error(e.getMessage(), e);
			return "该任务已经被暂停, 请检查暂停队列";
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			return "移动任务到暂停队列失败, error:" + e.getMessage();
		}

		try {
			appContext.getRepeatJobQueue().remove(jobId);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			return "删除Repeat任务失败，请手动删除! error:" + e.getMessage();
		}

		try {
			if (!jobPo.getRelyOnPrevCycle()) {
				appContext.getRepeatJobQueue().updateLastGenerateTriggerTime(jobPo.getJobId(), new Date().getTime());
				appContext.getExecutableJobQueue().removeBatch(jobPo.getRealTaskId(), jobPo.getTaskTrackerNodeGroup());
			} else {
				appContext.getExecutableJobQueue().remove(taskTrackerNodeGroup, jobId);
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			CatUtils.recordEvent(false, jobPo.getTaskId(), "SUSPEND_REPEAT_JOB");
			return "删除等待执行的任务失败，请手动删除! error:" + e.getMessage();
		}

		CatUtils.recordEvent(true, jobPo.getTaskId(), "SUSPEND_REPEAT_JOB");
		JobLogUtils.log(LogType.SUSPEND, jobPo, appContext.getJobLogger());

		return "ok";
	}

}
