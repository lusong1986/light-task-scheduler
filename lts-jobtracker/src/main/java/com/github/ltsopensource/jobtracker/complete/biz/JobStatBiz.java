package com.github.ltsopensource.jobtracker.complete.biz;

import java.util.List;

import com.github.ltsopensource.admin.request.JobQueueReq;
import com.github.ltsopensource.admin.response.PaginationRsp;
import com.github.ltsopensource.biz.logger.domain.JobLogPo;
import com.github.ltsopensource.biz.logger.domain.LogType;
import com.github.ltsopensource.core.commons.utils.CatUtils;
import com.github.ltsopensource.core.commons.utils.CollectionUtils;
import com.github.ltsopensource.core.constant.Constants;
import com.github.ltsopensource.core.constant.Level;
import com.github.ltsopensource.core.domain.Action;
import com.github.ltsopensource.core.domain.JobRunResult;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.core.protocol.command.JobCompletedRequest;
import com.github.ltsopensource.core.support.JobDomainConverter;
import com.github.ltsopensource.jobtracker.domain.JobTrackerAppContext;
import com.github.ltsopensource.jobtracker.monitor.JobTrackerMStatReporter;
import com.github.ltsopensource.queue.domain.JobGrayPo;
import com.github.ltsopensource.remoting.protocol.RemotingCommand;
import com.github.ltsopensource.remoting.protocol.RemotingProtos;

/**
 * 任务数据统计 Chain
 *
 * @author Robert HG (254963746@qq.com) on 11/11/15.
 */
public class JobStatBiz implements JobCompletedBiz {

	private final static Logger LOGGER = LoggerFactory.getLogger(JobStatBiz.class);

	private JobTrackerAppContext appContext;
	private JobTrackerMStatReporter stat;

	public JobStatBiz(JobTrackerAppContext appContext) {
		this.appContext = appContext;
		this.stat = (JobTrackerMStatReporter) appContext.getMStatReporter();
	}

	@Override
	public RemotingCommand doBiz(JobCompletedRequest request) {
		List<JobRunResult> results = request.getJobRunResults();
		if (CollectionUtils.isEmpty(results)) {
			return RemotingCommand.createResponseCommand(RemotingProtos.ResponseCode.REQUEST_PARAM_ERROR.code(),
					"JobResults can not be empty!");
		}

		LOGGER.info("Job execute completed : {}", results);

		LogType logType = request.isReSend() ? LogType.RESEND : LogType.FINISHED;
		for (JobRunResult result : results) {
			if (request.isReSend()) {
				CatUtils.recordEvent(true, result.getJobMeta().getRealTaskId(), "IS_RESEND_JOB_RESULT");
			}

			// 记录日志
			JobLogPo jobLogPo = JobDomainConverter.convertJobLog(result.getJobMeta());
			jobLogPo.setMsg(result.getMsg());
			jobLogPo.setLogType(logType);
			jobLogPo.setSuccess(Action.EXECUTE_SUCCESS.equals(result.getAction()));
			jobLogPo.setTaskTrackerIdentity(request.getIdentity());
			jobLogPo.setLevel(Level.INFO);
			jobLogPo.setLogTime(result.getTime());

			JobQueueReq jobQueueReq = new JobQueueReq();
			jobQueueReq.setTaskId(jobLogPo.getRealTaskId());
			PaginationRsp<JobGrayPo> grayPos = appContext.getJobGrayFlag().pageSelect(jobQueueReq);
			if (grayPos != null && grayPos.getResults() > 0) {
				final String gray = grayPos.getRows().get(0).getGray();
				jobLogPo.setParam(Constants.GRAY, gray);
			}

			appContext.getJobLogger().log(jobLogPo);

			// 监控数据统计
			if (result.getAction() != null) {
				switch (result.getAction()) {
				case EXECUTE_SUCCESS:
					CatUtils.recordEvent(true, result.getJobMeta().getRealTaskId(), Action.EXECUTE_SUCCESS.name());
					stat.incExeSuccessNum();
					break;
				case EXECUTE_FAILED:
					CatUtils.recordEvent(false, result.getJobMeta().getRealTaskId(), Action.EXECUTE_FAILED.name());
					stat.incExeFailedNum();
					break;
				case EXECUTE_LATER:
					CatUtils.recordEvent(false, result.getJobMeta().getRealTaskId(), Action.EXECUTE_LATER.name());
					stat.incExeLaterNum();
					break;
				case EXECUTE_EXCEPTION:
					CatUtils.recordEvent(false, result.getJobMeta().getRealTaskId(), Action.EXECUTE_EXCEPTION.name());
					stat.incExeExceptionNum();
					break;
				}
			}
		}
		return null;
	}

}
