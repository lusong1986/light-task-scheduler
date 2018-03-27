package com.github.ltsopensource.admin.web.api;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.github.ltsopensource.admin.cluster.BackendAppContext;
import com.github.ltsopensource.admin.request.JobQueueReq;
import com.github.ltsopensource.admin.response.PaginationRsp;
import com.github.ltsopensource.admin.web.AbstractMVC;
import com.github.ltsopensource.admin.web.support.Builder;
import com.github.ltsopensource.admin.web.vo.RestfulResponse;
import com.github.ltsopensource.biz.logger.JobLogUtils;
import com.github.ltsopensource.biz.logger.domain.LogType;
import com.github.ltsopensource.core.commons.utils.Assert;
import com.github.ltsopensource.core.commons.utils.CatUtils;
import com.github.ltsopensource.core.commons.utils.StringUtils;
import com.github.ltsopensource.core.constant.Constants;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.core.support.CronExpression;
import com.github.ltsopensource.core.support.CronExpressionUtils;
import com.github.ltsopensource.core.support.JobUtils;
import com.github.ltsopensource.core.support.SystemClock;
import com.github.ltsopensource.queue.domain.JobPo;
import com.github.ltsopensource.store.jdbc.exception.DupEntryException;

/**
 * @author Robert HG (254963746@qq.com) on 3/26/16.
 */
@RestController
public class SuspendJobQueueApi extends AbstractMVC {

	private static final Logger LOGGER = LoggerFactory.getLogger(SuspendJobQueueApi.class);

	@Autowired
	private BackendAppContext appContext;

	private ExecutorService recoveryThreadPool = Executors.newFixedThreadPool(10);

	@RequestMapping("/job-queue/suspend-job-get")
	public RestfulResponse suspendJobGet(JobQueueReq request) {
		PaginationRsp<JobPo> paginationRsp = appContext.getSuspendJobQueue().pageSelect(request);
		RestfulResponse response = new RestfulResponse();
		response.setSuccess(true);
		response.setResults(paginationRsp.getResults());
		response.setRows(paginationRsp.getRows());
		return response;
	}

	@RequestMapping("/job-queue/suspend-job-update")
	public RestfulResponse suspendJobUpdate(String jobType, JobQueueReq request) {
		// 检查参数
		try {
			Assert.hasLength(request.getJobId(), "jobId不能为空!");
			Assert.hasLength(jobType, "jobType不能为空!");
		} catch (IllegalArgumentException e) {
			return Builder.build(false, e.getMessage());
		}
		try {

			JobPo jobPo = appContext.getSuspendJobQueue().getJob(request.getJobId());

			if ("CRON".equals(jobType)) {
				// 检查参数
				try {
					Assert.hasLength(request.getCronExpression(), "cronExpression不能为空!");
				} catch (IllegalArgumentException e) {
					return Builder.build(false, e.getMessage());
				}
				// 1. 检测 cronExpression是否是正确的
				CronExpression expression = new CronExpression(request.getCronExpression());
				if (expression.getTimeAfter(new Date()) == null) {
					return Builder.build(
							false,
							StringUtils.format("该CronExpression={} 已经没有执行时间点! 请重新设置或者直接删除。",
									request.getCronExpression()));
				}
				// 看CronExpression是否有修改,如果有修改,需要更新triggerTime
				if (!request.getCronExpression().equals(jobPo.getCronExpression())) {
					request.setTriggerTime(expression.getTimeAfter(new Date()));
				}
			} else {
				try {
					Assert.notNull(request.getRepeatInterval(), "repeatInterval不能为空!");
					Assert.isTrue(request.getRepeatInterval() > 0, "repeatInterval必须大于0");
					Assert.isTrue(request.getRepeatCount() >= -1, "repeatCount必须>= -1");
				} catch (IllegalArgumentException e) {
					return Builder.build(false, e.getMessage());
				}
				// 如果repeatInterval有修改,需要把triggerTime也要修改下
				if (!request.getRepeatInterval().equals(jobPo.getRepeatInterval())) {
					long nextTriggerTime = JobUtils.getRepeatNextTriggerTime(jobPo);
					request.setTriggerTime(new Date(nextTriggerTime));
				}
				request.setCronExpression(null);
			}

			boolean success = appContext.getSuspendJobQueue().selectiveUpdateByJobId(request);
			if (success) {
				CatUtils.recordEvent(true, jobPo.getTaskId(), "UPDATE_SUSPEND_JOB");
				JobLogUtils.log(LogType.UPDATE, jobPo, appContext.getJobLogger());
				return Builder.build(true);
			} else {
				CatUtils.recordEvent(false, jobPo.getTaskId(), "UPDATE_SUSPEND_JOB");
				return Builder.build(false, "该任务已经被删除或者执行完成");
			}
		} catch (ParseException e) {
			LOGGER.error(e.getMessage(), e);
			return Builder.build(false, "请输入正确的 CronExpression");
		}
	}

	@RequestMapping("/job-queue/suspend-job-delete")
	public RestfulResponse suspendJobDelete(JobQueueReq request) {
		if (StringUtils.isEmpty(request.getJobId())) {
			return Builder.build(false, "JobId 必须传!");
		}

		JobPo jobPo = appContext.getSuspendJobQueue().getJob(request.getJobId());
		if (jobPo == null) {
			return Builder.build(true, "已经删除");
		}

		boolean success = appContext.getSuspendJobQueue().remove(request.getJobId());
		if (success) {
			JobLogUtils.log(LogType.DEL, jobPo, appContext.getJobLogger());
			CatUtils.recordEvent(true, jobPo.getTaskId(), "DEL_SUSPEND_JOB");
		}

		appContext.getJobGrayFlag().remove(jobPo.getTaskId());
		appContext.getExecutableJobDependency().remove(jobPo.getJobId());
		appContext.getExecutableJobDependency().removeDependency(jobPo.getJobId(), jobPo.getTaskTrackerNodeGroup());

		return Builder.build(success);
	}

	@RequestMapping("/job-queue/suspend-job-recovery")
	public RestfulResponse suspendJobRecovery(JobQueueReq request) {
		if (StringUtils.isEmpty(request.getJobId())) {
			return Builder.build(false, "JobId 必须传!");
		}

		JobPo jobPo = appContext.getSuspendJobQueue().getJob(request.getJobId());
		if (jobPo == null) {
			CatUtils.recordEvent(false, request.getJobId(), "RESUME_SUSPEND_JOB");
			return Builder.build(false, "任务不存在，或者已经删除");
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
					return Builder.build(false, "Cron队列中任务已经存在，请检查");
				} catch (Exception e) {
					LOGGER.error(e.getMessage(), e);
					CatUtils.recordEvent(false, jobPo.getTaskId(), "RESUME_SUSPEND_JOB");
					return Builder.build(false, "插入Cron队列中任务错误, error:" + e.getMessage());
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
						return Builder.build(false, "等待执行队列中任务已经存在，请检查");
					} catch (Exception e) {
						LOGGER.error(e.getMessage(), e);
						CatUtils.recordEvent(false, jobPo.getTaskId(), "RESUME_SUSPEND_JOB");
						return Builder.build(false, "插入等待执行队列中任务错误, error:" + e.getMessage());
					}
				} else {
					// 不依赖上一周期的
					appContext.getNoRelyJobGenerator().generateCronJobForInterval(jobPo, new Date(SystemClock.now()));
				}

			} else {
				CatUtils.recordEvent(false, jobPo.getTaskId(), "RESUME_SUSPEND_JOB");
				return Builder.build(false, "该任务已经无效, 或者已经没有下一轮执行时间点, 请直接删除");
			}
		} else if (jobPo.isRepeatable()) {
			// 先恢复,才能删除
			if (jobPo.getRepeatCount() == -1 || jobPo.getRepeatedCount() < jobPo.getRepeatCount()) {
				jobPo.setGmtModified(SystemClock.now());
				try {
					// 1.add to cron job queue
					appContext.getRepeatJobQueue().add(jobPo);
				} catch (DupEntryException e) {
					LOGGER.error(e.getMessage(), e);
					CatUtils.recordEvent(false, jobPo.getTaskId(), "RESUME_SUSPEND_JOB");
					return Builder.build(false, "Repeat队列中任务已经存在，请检查");
				} catch (Exception e) {
					LOGGER.error(e.getMessage(), e);
					CatUtils.recordEvent(false, jobPo.getTaskId(), "RESUME_SUSPEND_JOB");
					return Builder.build(false, "插入Repeat队列中任务错误, error:" + e.getMessage());
				}

				if (jobPo.getRelyOnPrevCycle()) {
					try {
						// 2. add to executable queue
						JobPo repeatJob = appContext.getRepeatJobQueue().getJob(request.getJobId());
						long nextTriggerTime = JobUtils.getRepeatNextTriggerTime(repeatJob);
						jobPo.setTriggerTime(nextTriggerTime);
						jobPo.setInternalExtParam(Constants.EXE_SEQ_ID, JobUtils.generateExeSeqId(jobPo));
						appContext.getExecutableJobQueue().add(jobPo);
					} catch (DupEntryException e) {
						LOGGER.error(e.getMessage(), e);
						CatUtils.recordEvent(false, jobPo.getTaskId(), "RESUME_SUSPEND_JOB");
						return Builder.build(false, "等待执行队列中任务已经存在，请检查");
					} catch (Exception e) {
						LOGGER.error(e.getMessage(), e);
						CatUtils.recordEvent(false, jobPo.getTaskId(), "RESUME_SUSPEND_JOB");
						return Builder.build(false, "插入等待执行队列中任务错误, error:" + e.getMessage());
					}
				} else {
					// 不依赖上一周期的
					appContext.getNoRelyJobGenerator().generateRepeatJobForInterval(jobPo, new Date(SystemClock.now()));
				}
			} else {
				CatUtils.recordEvent(false, jobPo.getTaskId(), "RESUME_SUSPEND_JOB");
				return Builder.build(false, "该任务已经无效, 或者已经没有下一轮执行时间点, 请直接删除");
			}
		}

		// 从暂停表中移除
		if (!appContext.getSuspendJobQueue().remove(request.getJobId())) {
			CatUtils.recordEvent(false, jobPo.getTaskId(), "RESUME_SUSPEND_JOB");
			return Builder.build(false, "恢复暂停任务失败，请重试");
		}

		CatUtils.recordEvent(true, jobPo.getTaskId(), "RESUME_SUSPEND_JOB");
		JobLogUtils.log(LogType.RESUME, jobPo, appContext.getJobLogger());
		return Builder.build(true);
	}

	@RequestMapping("/job-queue/suspend-job-recovery-all")
	public RestfulResponse suspendJobRecoveryAll(JobQueueReq request) {
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

		Map<String, Future<String>> map = new HashMap<String, Future<String>>();
		LOGGER.info("start recovery all suspend jobs:" + suspendJobs.size());
		for (final JobPo jobPo : suspendJobs) {
			Future<String> future = recoveryThreadPool.submit(new Callable<String>() {
				public String call() throws Exception {
					String ret = suspendJobRecovery(jobPo.getJobId());
					LOGGER.info(jobPo.getTaskId() + " recovery result :" + ret);
					return ret;
				}
			});
			map.put(jobPo.getJobId(), future);
		}

		for (final JobPo jobPo : suspendJobs) {
			try {
				map.get(jobPo.getJobId()).get();
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
			}
		}

		LOGGER.info("end recovery all suspend jobs");
		return Builder.build(true, "add job gray succeed");
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
					appContext.getNoRelyJobGenerator().generateCronJobForInterval(jobPo, new Date(SystemClock.now()));
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
					appContext.getNoRelyJobGenerator().generateRepeatJobForInterval(jobPo, new Date(SystemClock.now()));
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

}
