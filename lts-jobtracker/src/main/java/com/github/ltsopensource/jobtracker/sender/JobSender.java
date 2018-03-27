package com.github.ltsopensource.jobtracker.sender;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.fastjson.JSONObject;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import com.github.ltsopensource.admin.request.JobQueueReq;
import com.github.ltsopensource.admin.response.PaginationRsp;
import com.github.ltsopensource.biz.logger.domain.JobLogPo;
import com.github.ltsopensource.biz.logger.domain.LogType;
import com.github.ltsopensource.core.commons.utils.CatUtils;
import com.github.ltsopensource.core.commons.utils.StringUtils;
import com.github.ltsopensource.core.constant.Constants;
import com.github.ltsopensource.core.constant.Level;
import com.github.ltsopensource.core.json.JSON;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.core.support.JobDomainConverter;
import com.github.ltsopensource.core.support.SystemClock;
import com.github.ltsopensource.jobtracker.domain.JobTrackerAppContext;
import com.github.ltsopensource.queue.domain.JobGrayPo;
import com.github.ltsopensource.queue.domain.JobPo;
import com.github.ltsopensource.store.jdbc.exception.DupEntryException;

/**
 * @author Robert HG (254963746@qq.com) on 11/11/15.
 */
public class JobSender {

	private final Logger LOGGER = LoggerFactory.getLogger(JobSender.class);

	private JobTrackerAppContext appContext;

	public JobSender(JobTrackerAppContext appContext) {
		this.appContext = appContext;
	}

	public SendResult send(String taskTrackerNodeGroup, String taskTrackerIdentity, int size, SendInvoker invoker) {
		JobPosTranscation jobPosTranscation = fetchJob(taskTrackerNodeGroup, taskTrackerIdentity, size);
		List<JobPo> jobPos = jobPosTranscation.getJobPos();
		if (jobPos.size() == 0) {
			return new SendResult(false, JobPushResult.NO_JOB);
		}

		final Transaction transaction = jobPosTranscation.getTransaction();
		SendResult sendResult = null;
		try {
			sendResult = invoker.invoke(jobPos);
			if (sendResult.isSuccess()) {

				if (null != transaction) {
					final JSONObject ret = new JSONObject();
					for (JobPo jobPo : jobPos) {
						final JSONObject jobJson = new JSONObject();
						jobJson.put("jobId", jobPo.getJobId());
						jobJson.put("realTaskId", jobPo.getRealTaskId());
						jobJson.put("taskTrackerIdentity", jobPo.getTaskTrackerIdentity());
						jobJson.put("taskTrackerNodeGroup", jobPo.getTaskTrackerNodeGroup());
						jobJson.put("triggerTime", jobPo.getTriggerTime());
						ret.put(jobPo.getRealTaskId(), jobJson);
					}
					transaction.addData("jobPos", ret.toJSONString());
				}

				List<JobLogPo> jobLogPos = new ArrayList<JobLogPo>(jobPos.size());
				for (JobPo jobPo : jobPos) {
					// 记录日志
					JobLogPo jobLogPo = JobDomainConverter.convertJobLog(jobPo);
					jobLogPo.setSuccess(true);
					jobLogPo.setLogType(LogType.SENT);
					jobLogPo.setLogTime(SystemClock.now());
					jobLogPo.setLevel(Level.INFO);

					JobQueueReq request = new JobQueueReq();
					request.setTaskId(jobLogPo.getRealTaskId());
					PaginationRsp<JobGrayPo> grayPos = appContext.getJobGrayFlag().pageSelect(request);
					if (grayPos != null && grayPos.getResults() > 0) {
						final String gray = grayPos.getRows().get(0).getGray();
						jobLogPo.setParam(Constants.GRAY, gray);
					}

					jobLogPos.add(jobLogPo);
				}
				appContext.getJobLogger().log(jobLogPos);

				if (null != transaction) {
					CatUtils.catSuccess(transaction);
				}
			}
		} catch (Exception e) {
			if (null != transaction) {
				CatUtils.catException(transaction, e);
			}
		} finally {
			if (null != transaction) {
				CatUtils.catComplete(transaction);
			}
		}

		return sendResult;
	}

	private JobPosTranscation fetchJob(String taskTrackerNodeGroup, String taskTrackerIdentity, int size) {
		Transaction transaction = null;
		List<JobPo> jobPos = new ArrayList<JobPo>(size);

		for (int i = 0; i < size; i++) {
			// 从mongo\mysql 中取一个可运行的job
			final JobPo jobPo = appContext.getPreLoader().take(taskTrackerNodeGroup, taskTrackerIdentity);
			if (jobPo == null) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Job push failed: no job! nodeGroup=" + taskTrackerNodeGroup + ", identity="
							+ taskTrackerIdentity);
				}
				break;
			}

			if (null == transaction) {
				String messageId = jobPo.getInternalExtParam(Cat.Context.CHILD + 1);
				String parentId = jobPo.getInternalExtParam(Cat.Context.PARENT);
				String rootId = jobPo.getInternalExtParam(Cat.Context.ROOT);
				if (StringUtils.isNotEmpty(messageId, parentId, rootId)) {
					CatUtils.buildContext(messageId, parentId, rootId);
				}
				transaction = CatUtils.catTransaction("JOB_SENDER", jobPo.getRealTaskId());
			}

			// 传递new trace给JOB_RUNNER, executingdeadjob
			final Map<String, String> newTraceMap = CatUtils.getContext();
			for (Entry<String, String> entry : newTraceMap.entrySet()) {
				jobPo.setInternalExtParam(entry.getKey(), entry.getValue());
			}

			// IMPORTANT: 这里要先切换队列
			try {
				appContext.getExecutingJobQueue().add(jobPo);
			} catch (DupEntryException e) {
				LOGGER.warn("ExecutingJobQueue already exist:" + JSON.toJSONString(jobPo));
				appContext.getExecutableJobQueue().resume(jobPo);
				continue;
			}
			appContext.getExecutableJobQueue().remove(jobPo.getTaskTrackerNodeGroup(), jobPo.getJobId());

			jobPos.add(jobPo);
		}

		JobPosTranscation jobPosTranscation = new JobPosTranscation();
		jobPosTranscation.setJobPos(jobPos);
		jobPosTranscation.setTransaction(transaction);
		return jobPosTranscation;
	}

	class JobPosTranscation {
		Transaction transaction = null;
		List<JobPo> jobPos = new ArrayList<JobPo>();

		public JobPosTranscation() {
		}

		public Transaction getTransaction() {
			return transaction;
		}

		public void setTransaction(Transaction transaction) {
			this.transaction = transaction;
		}

		public List<JobPo> getJobPos() {
			return jobPos;
		}

		public void setJobPos(List<JobPo> jobPos) {
			this.jobPos = jobPos;
		}

	}

	public interface SendInvoker {
		SendResult invoke(List<JobPo> jobPos);
	}

	public static class SendResult {
		private boolean success;
		private Object returnValue;

		public SendResult(boolean success, Object returnValue) {
			this.success = success;
			this.returnValue = returnValue;
		}

		public boolean isSuccess() {
			return success;
		}

		public void setSuccess(boolean success) {
			this.success = success;
		}

		public Object getReturnValue() {
			return returnValue;
		}

		public void setReturnValue(Object returnValue) {
			this.returnValue = returnValue;
		}
	}

}
