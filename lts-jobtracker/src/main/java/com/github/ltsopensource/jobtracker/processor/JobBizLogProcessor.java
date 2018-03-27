package com.github.ltsopensource.jobtracker.processor;

import java.util.List;

import com.github.ltsopensource.admin.request.JobQueueReq;
import com.github.ltsopensource.admin.response.PaginationRsp;
import com.github.ltsopensource.biz.logger.domain.JobLogPo;
import com.github.ltsopensource.biz.logger.domain.LogType;
import com.github.ltsopensource.core.commons.utils.CatUtils;
import com.github.ltsopensource.core.commons.utils.CollectionUtils;
import com.github.ltsopensource.core.constant.Constants;
import com.github.ltsopensource.core.domain.BizLog;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.core.protocol.JobProtos;
import com.github.ltsopensource.core.protocol.command.BizLogSendRequest;
import com.github.ltsopensource.core.support.SystemClock;
import com.github.ltsopensource.jobtracker.domain.JobTrackerAppContext;
import com.github.ltsopensource.queue.domain.JobGrayPo;
import com.github.ltsopensource.remoting.Channel;
import com.github.ltsopensource.remoting.exception.RemotingCommandException;
import com.github.ltsopensource.remoting.protocol.RemotingCommand;

/**
 * @author Robert HG (254963746@qq.com) on 3/30/15.
 */
public class JobBizLogProcessor extends AbstractRemotingProcessor {

	private static final Logger LOGGER = LoggerFactory.getLogger(JobBizLogProcessor.class);

	public JobBizLogProcessor(JobTrackerAppContext appContext) {
		super(appContext);
	}

	@Override
	public RemotingCommand processRequest(Channel channel, RemotingCommand request) throws RemotingCommandException {
		BizLogSendRequest requestBody = request.getBody();

		List<BizLog> bizLogs = requestBody.getBizLogs();
		if (CollectionUtils.isNotEmpty(bizLogs)) {
			for (BizLog bizLog : bizLogs) {
				try {
					JobLogPo jobLogPo = new JobLogPo();
					jobLogPo.setGmtCreated(SystemClock.now());
					jobLogPo.setLogTime(bizLog.getLogTime());
					jobLogPo.setTaskTrackerNodeGroup(bizLog.getTaskTrackerNodeGroup());
					jobLogPo.setTaskTrackerIdentity(bizLog.getTaskTrackerIdentity());
					jobLogPo.setJobId(bizLog.getJobId());
					jobLogPo.setTaskId(bizLog.getTaskId());
					jobLogPo.setRealTaskId(bizLog.getRealTaskId());
					jobLogPo.setJobType(bizLog.getJobType());
					jobLogPo.setMsg(bizLog.getMsg());
					jobLogPo.setSuccess(true);
					jobLogPo.setLevel(bizLog.getLevel());
					jobLogPo.setLogType(LogType.BIZ);

					JobQueueReq jobQueueReq = new JobQueueReq();
					jobQueueReq.setTaskId(jobLogPo.getRealTaskId());
					PaginationRsp<JobGrayPo> grayPos = appContext.getJobGrayFlag().pageSelect(jobQueueReq);
					if (grayPos != null && grayPos.getResults() > 0) {
						final String gray = grayPos.getRows().get(0).getGray();
						jobLogPo.setParam(Constants.GRAY, gray);
					}

					appContext.getJobLogger().log(jobLogPo);
					CatUtils.recordEvent(true, bizLog.getRealTaskId(), JobProtos.RequestCode.BIZ_LOG_SEND.name());
				} catch (Exception e) {
					LOGGER.warn("bizlog warn " + e.getMessage(), e);
				}
			}
		}

		return RemotingCommand.createResponseCommand(JobProtos.ResponseCode.BIZ_LOG_SEND_SUCCESS.code(), "");
	}
}
