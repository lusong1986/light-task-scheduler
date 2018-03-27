package com.github.ltsopensource.queue.support;

import java.util.Date;

import com.github.ltsopensource.core.commons.utils.DateUtils;
import com.github.ltsopensource.core.constant.Constants;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.core.support.CronExpressionUtils;
import com.github.ltsopensource.core.support.JobUtils;
import com.github.ltsopensource.queue.CronJobQueue;
import com.github.ltsopensource.queue.ExecutableJobQueue;
import com.github.ltsopensource.queue.RepeatJobQueue;
import com.github.ltsopensource.queue.domain.JobPo;
import com.github.ltsopensource.store.jdbc.exception.DupEntryException;

/**
 * @author Robert HG (254963746@qq.com) on 4/6/16.
 */
public class NonRelyJobUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(NonRelyJobUtils.class);

	/**
	 * 生成一个小时的任务
	 */
	public static void addCronJobForInterval(ExecutableJobQueue executableJobQueue, CronJobQueue cronJobQueue,
			int scheduleIntervalMinute, final JobPo finalJobPo, Date lastGenerateTime) {
		final JobPo jobPo = JobUtils.copy(finalJobPo);

		final String cronExpression = jobPo.getCronExpression();
		final long endTime = DateUtils.addMinute(lastGenerateTime, scheduleIntervalMinute).getTime();
		Date timeAfter = lastGenerateTime;
		boolean stop = false;
		while (!stop) {
			final Date nextTriggerTime = CronExpressionUtils.getNextTriggerTime(cronExpression, timeAfter);
			if (nextTriggerTime == null) {
				stop = true;
			} else {
				if (nextTriggerTime.getTime() > lastGenerateTime.getTime() && nextTriggerTime.getTime() <= endTime) {
					// cron任务从上次lastGenerateTime之后重新
					jobPo.setTriggerTime(nextTriggerTime.getTime());
					jobPo.setJobId(JobUtils.generateJobId());
					jobPo.setTaskId(finalJobPo.getTaskId() + "_" + DateUtils.format(nextTriggerTime, "MMdd-HHmmss"));
					jobPo.setInternalExtParam(Constants.ONCE, Boolean.TRUE.toString());
					try {
						jobPo.setInternalExtParam(Constants.EXE_SEQ_ID, JobUtils.generateExeSeqId(jobPo));
						executableJobQueue.add(jobPo);
					} catch (DupEntryException e) {
						LOGGER.warn("Cron Job[taskId={}, taskTrackerNodeGroup={}] Already Exist in ExecutableJobQueue",
								jobPo.getTaskId(), jobPo.getTaskTrackerNodeGroup());
					}
				} else {
					stop = true;
				}
			}
			timeAfter = nextTriggerTime;
		}

		cronJobQueue.updateLastGenerateTriggerTime(finalJobPo.getJobId(), endTime);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Add CronJob {} to {}", jobPo, DateUtils.formatYMD_HMS(new Date(endTime)));
		}
	}

	public static void addRepeatJobForInterval(ExecutableJobQueue executableJobQueue, RepeatJobQueue repeatJobQueue,
			final int scheduleIntervalMinute, final JobPo finalJobPo, final Date lastGenerateTime) {
		final JobPo jobPo = JobUtils.copy(finalJobPo);
		final long firstTriggerTime = Long.valueOf(jobPo.getInternalExtParam(Constants.FIRST_FIRE_TIME));

		final Long repeatInterval = jobPo.getRepeatInterval();
		final Integer repeatCount = jobPo.getRepeatCount();

		final long endTime = DateUtils.addMinute(lastGenerateTime, scheduleIntervalMinute).getTime();
		if (endTime <= firstTriggerTime) {
			return;
		}

		// 计算出已经生成的可执行job 起始repeatedCount
		long initTime = lastGenerateTime.getTime();
		int repeatedCount = Long.valueOf((initTime - firstTriggerTime) / jobPo.getRepeatInterval()).intValue();
		if (repeatedCount <= 0) {
			repeatedCount = 1;
		}

		final JobPo repeatJobPjo = repeatJobQueue.getJob(jobPo.getTaskTrackerNodeGroup(), jobPo.getTaskId());

		boolean stop = false;
		while (!stop) {
			final Long nextTriggerTime = firstTriggerTime + (repeatedCount - 1) * repeatInterval;

			if (repeatJobPjo.getLastGenerateTriggerTime() == null || repeatJobPjo.getLastGenerateTriggerTime() == 0) {
				// 说明是第一次生成executable job
				initTime = 0;
			}

			if ((nextTriggerTime > initTime) && (nextTriggerTime <= endTime)
					&& (repeatCount == -1 || repeatedCount <= repeatCount)) {
				// 添加任务
				jobPo.setTriggerTime(nextTriggerTime);
				jobPo.setJobId(JobUtils.generateJobId());
				jobPo.setTaskId(finalJobPo.getTaskId() + "_"
						+ DateUtils.format(new Date(nextTriggerTime), "MMdd-HHmmss"));
				jobPo.setRepeatedCount(repeatedCount);
				jobPo.setInternalExtParam(Constants.ONCE, Boolean.TRUE.toString());
				try {
					jobPo.setInternalExtParam(Constants.EXE_SEQ_ID, JobUtils.generateExeSeqId(jobPo));
					executableJobQueue.add(jobPo);
				} catch (DupEntryException e) {
					LOGGER.warn("Repeat Job[taskId={}, taskTrackerNodeGroup={}] Already Exist in ExecutableJobQueue",
							jobPo.getTaskId(), jobPo.getTaskTrackerNodeGroup());
				}
				repeatedCount++;
			} else if (repeatedCount < repeatCount) {
				repeatedCount++;
			} else {
				stop = true;
			}
		}

		// 更新时间
		repeatJobQueue.updateLastGenerateTriggerTime(finalJobPo.getJobId(), endTime);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Add RepeatJob {} to {}", jobPo, DateUtils.formatYMD_HMS(new Date(endTime)));
		}
	}
}
