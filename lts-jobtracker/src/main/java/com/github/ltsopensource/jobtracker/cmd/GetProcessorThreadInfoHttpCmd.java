package com.github.ltsopensource.jobtracker.cmd;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import com.github.ltsopensource.cmd.HttpCmdProc;
import com.github.ltsopensource.cmd.HttpCmdRequest;
import com.github.ltsopensource.cmd.HttpCmdResponse;
import com.github.ltsopensource.core.cmd.HttpCmdNames;
import com.github.ltsopensource.core.domain.Pair;
import com.github.ltsopensource.core.json.JSON;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.jobtracker.domain.JobTrackerAppContext;
import com.github.ltsopensource.remoting.AbstractRemotingServer;
import com.github.ltsopensource.remoting.RemotingProcessor;

/**
 * 查询jobtracker内部的线程池的信息
 *
 * @author lusong
 */
public class GetProcessorThreadInfoHttpCmd implements HttpCmdProc {

	private static final Logger LOGGER = LoggerFactory.getLogger(GetProcessorThreadInfoHttpCmd.class);

	private JobTrackerAppContext appContext;

	public GetProcessorThreadInfoHttpCmd(JobTrackerAppContext appContext) {
		this.appContext = appContext;
	}

	@Override
	public String nodeIdentity() {
		return appContext.getConfig().getIdentity();
	}

	@Override
	public String getCommand() {
		return HttpCmdNames.HTTP_CMD_GET_PROCESSOR_THREAD_INFO_NODE;
	}

	@Override
	public HttpCmdResponse execute(HttpCmdRequest request) throws Exception {
		HttpCmdResponse response = new HttpCmdResponse();
		response.setSuccess(false);

		try {
			final Map<String, Integer> threadInfoMap = new ConcurrentHashMap<String, Integer>();

			// async thread pool, process async call's response
			AbstractRemotingServer abstractRemotingServer = (AbstractRemotingServer) appContext.getRemotingServer()
					.getRemotingServer();
			final ThreadPoolExecutor publicExecutor = (ThreadPoolExecutor) abstractRemotingServer.getCallbackExecutor();

			threadInfoMap.put("asyncActiveCount", publicExecutor.getActiveCount());
			threadInfoMap.put("asyncCorePoolSize", publicExecutor.getCorePoolSize());
			threadInfoMap.put("asyncMaximumPoolSize", publicExecutor.getMaximumPoolSize());
			threadInfoMap.put("asyncQueueSize", publicExecutor.getQueue().size());

			// sync thread pool, process all request
			final Pair<RemotingProcessor, ExecutorService> defaultRequestProcessor = abstractRemotingServer
					.getDefaultRequestProcessor();
			final ThreadPoolExecutor syncThreadPool = (ThreadPoolExecutor) defaultRequestProcessor.getValue();

			threadInfoMap.put("syncActiveCount", syncThreadPool.getActiveCount());
			threadInfoMap.put("syncCorePoolSize", syncThreadPool.getCorePoolSize());
			threadInfoMap.put("syncMaximumPoolSize", syncThreadPool.getMaximumPoolSize());
			threadInfoMap.put("syncQueueSize", syncThreadPool.getQueue().size());

			// job pusher thread pool
			final ThreadPoolExecutor jobPusherExecutorService = (ThreadPoolExecutor) appContext
					.getPushExecutorService();
			threadInfoMap.put("jobPusherActiveCount", jobPusherExecutorService.getActiveCount());
			threadInfoMap.put("jobPusherCorePoolSize", jobPusherExecutorService.getCorePoolSize());
			threadInfoMap.put("jobPusherMaximumPoolSize", jobPusherExecutorService.getMaximumPoolSize());
			threadInfoMap.put("jobPusherQueueSize", jobPusherExecutorService.getQueue().size());

			// http comand thread pool
			final ThreadPoolExecutor httpCmdExecutorService = (ThreadPoolExecutor) appContext.getHttpCmdServer()
					.getAcceptor().getExecutorService();

			threadInfoMap.put("httpActiveCount", httpCmdExecutorService.getActiveCount());
			threadInfoMap.put("httpCorePoolSize", httpCmdExecutorService.getCorePoolSize());
			threadInfoMap.put("httpMaximumPoolSize", httpCmdExecutorService.getMaximumPoolSize());
			threadInfoMap.put("httpQueueSize", httpCmdExecutorService.getQueue().size());

			response.setObj(JSON.toJSONString(threadInfoMap));
			LOGGER.info("get jobtracker node thread pool :" + JSON.toJSONString(threadInfoMap));

			response.setSuccess(true);
		} catch (Exception e) {
			LOGGER.error("get jobtracker node thread pool error, message:", e);
			response.setMsg(e.getMessage());
		}
		return response;
	}

}
