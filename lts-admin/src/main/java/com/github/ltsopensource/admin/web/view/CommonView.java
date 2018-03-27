package com.github.ltsopensource.admin.web.view;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import com.github.ltsopensource.admin.cluster.BackendAppContext;
import com.github.ltsopensource.admin.request.JobQueueReq;
import com.github.ltsopensource.admin.response.PaginationRsp;
import com.github.ltsopensource.biz.logger.domain.LogType;
import com.github.ltsopensource.core.cluster.NodeType;
import com.github.ltsopensource.core.commons.utils.DateUtils;
import com.github.ltsopensource.core.logger.Level;
import com.github.ltsopensource.core.support.CronExpressionUtils;
import com.github.ltsopensource.queue.domain.JobPo;
import com.github.ltsopensource.queue.domain.NodeGroupPo;

@Controller
public class CommonView {

	private static final String SPLIT_CHAR = "#";

	@Autowired
	private BackendAppContext appContext;

	@RequestMapping("index")
	public String index() {
		return "index";
	}

	@RequestMapping("node-manager")
	public String nodeManagerUI() {
		return "nodeManager";
	}

	@RequestMapping("node-group-manager")
	public String nodeGroupManagerUI() {
		return "nodeGroupManager";
	}

	@RequestMapping("node-onoffline-log")
	public String nodeOnOfflineLogUI(Model model) {
		model.addAttribute("startLogTime", DateUtils.formatYMD_HMS(DateUtils.addDay(new Date(), -3)));
		model.addAttribute("endLogTime", DateUtils.formatYMD_HMS(new Date()));
		return "nodeOnOfflineLog";
	}

	@RequestMapping("node-jvm-info")
	public String nodeJVMInfo(Model model, String identity) {
		model.addAttribute("identity", identity);
		return "nodeJvmInfo";
	}

	@RequestMapping("job-add")
	public String addJobUI(Model model) {
		setAttr(model);
		return "jobAdd";
	}

	@RequestMapping("timeline")
	public String timelineUI(Model model) {
		setAttr(model);

		final HashMap<String, Integer> yAxisMap = new HashMap<String, Integer>();
		model.addAttribute("appString", getAllApps(yAxisMap));

		final List<JobPo> cronJobs = getAllCronJobs();

		final ConcurrentHashMap<String, String> jobTimeMap = getCronJobsCoordinate(yAxisMap, cronJobs);

		final StringBuilder sb = new StringBuilder();
		final Set<Entry<String, String>> entrySet = jobTimeMap.entrySet();
		for (Entry<String, String> entry : entrySet) {
			String[] arr = entry.getKey().split(SPLIT_CHAR);
			String[] arr2 = entry.getValue().split(SPLIT_CHAR);

			sb.append("[");
			sb.append(arr[0]);
			sb.append(",");
			sb.append(arr[1]);
			sb.append(",");
			sb.append(arr2[0]);
			sb.append(",");
			sb.append(arr2[1]);
			sb.append("]");
			sb.append(";");
		}
		model.addAttribute("timeData", StringUtils.chomp(sb.toString(), ";"));

		return "timeline";
	}

	private ConcurrentHashMap<String, String> getCronJobsCoordinate(final HashMap<String, Integer> yAxisMap,
			final List<JobPo> cronJobs) {
		final ConcurrentHashMap<String, String> jobTimeMap = new ConcurrentHashMap<String, String>();
		final Calendar today = Calendar.getInstance();
		final int TODAY_DAY = today.get(Calendar.DAY_OF_MONTH);

		final Calendar morning = Calendar.getInstance();
		morning.set(Calendar.HOUR_OF_DAY, 0);
		morning.set(Calendar.MINUTE, 0);

		final ConcurrentHashMap<String, AtomicInteger> jobTimeCounter = new ConcurrentHashMap<String, AtomicInteger>();

		final GregorianCalendar jobTimeCalendar = new GregorianCalendar();
		for (final JobPo jobPo : cronJobs) {
			if (!checkTime(jobPo)) {
				continue;
			}

			double factor = 0.3;
			if (jobPo.getTaskTrackerNodeGroup().equals("inspection_task_tracker")) {
				factor = 0.2;
			}

			Date timeAfter = morning.getTime();
			while (true) {
				final Date nextTriggerTime = CronExpressionUtils.getNextTriggerTime(jobPo.getCronExpression(),
						timeAfter);
				jobTimeCalendar.setTimeInMillis(nextTriggerTime.getTime());

				if (TODAY_DAY == jobTimeCalendar.get(Calendar.DAY_OF_MONTH)) {
					int hour = jobTimeCalendar.get(Calendar.HOUR_OF_DAY); // 0-23
					int realMinute = jobTimeCalendar.get(Calendar.MINUTE);
					double min = ((double) realMinute) / 60;
					double minute = ((int) (min * 100)) / 100.0;

					final String realTime = hour + ":" + ((realMinute < 10) ? "0" + realMinute : realMinute);
					final double x = hour + minute;
					double y = yAxisMap.get(jobPo.getTaskTrackerNodeGroup());

					final String coordinateKey = x + SPLIT_CHAR + y;
					AtomicInteger oldCounter = jobTimeCounter.putIfAbsent(coordinateKey, new AtomicInteger(1));
					if (oldCounter != null) {
						oldCounter.incrementAndGet();
					}

					String old = jobTimeMap.putIfAbsent(coordinateKey, realTime + SPLIT_CHAR + jobPo.getRealTaskId());
					if (old != null) {
						y += (jobTimeCounter.get(coordinateKey).intValue() - 1) * factor;
						jobTimeMap.put(x + SPLIT_CHAR + y, realTime + SPLIT_CHAR + jobPo.getRealTaskId());
					}
					timeAfter = nextTriggerTime;
				} else {
					break;
				}
			}
		}

		return jobTimeMap;
	}

	private List<JobPo> getAllCronJobs() {
		final int limit = 100;
		final List<JobPo> cronJobs = new LinkedList<JobPo>();
		for (int i = 1;; i++) {
			int start = (i - 1) * limit;
			JobQueueReq req = new JobQueueReq();
			req.setStart(start);
			req.setLimit(limit);
			PaginationRsp<JobPo> paginationRsp = appContext.getCronJobQueue().pageSelect(req);
			if (paginationRsp != null && paginationRsp.getRows().size() > 0) {
				cronJobs.addAll(paginationRsp.getRows());
			} else {
				break;
			}
		}
		return cronJobs;
	}

	private String getAllApps(final HashMap<String, Integer> yAxisMap) {
		final List<NodeGroupPo> taskTrackers = appContext.getNodeGroupStore().getNodeGroup(NodeType.TASK_TRACKER);

		Comparator<NodeGroupPo> c = new Comparator<NodeGroupPo>() {

			@Override
			public int compare(NodeGroupPo n1, NodeGroupPo n2) {
				return n2.getName().compareTo(n1.getName());
			}
		};
		Collections.sort(taskTrackers, c);

		StringBuilder apps = new StringBuilder();
		for (int i = 0; i < taskTrackers.size(); i++) {
			final NodeGroupPo node = taskTrackers.get(i);
			final String nodeName = node.getName();

			String app = nodeName;
			if (nodeName.contains("_")) {
				app = nodeName.substring(0, nodeName.indexOf("_"));

				if (apps.toString().contains(app + ",")) {
					app = nodeName;
				}
			}
			apps.append(app);

			if (i < taskTrackers.size() - 1) {
				apps.append(",");
			}
			yAxisMap.put(nodeName, i);
		}
		return apps.toString();
	}

	private boolean checkTime(JobPo jobPo) {
		final Date nextTriggerTime = CronExpressionUtils.getNextTriggerTime(jobPo.getCronExpression());
		final Date nextNextTriggerTime = CronExpressionUtils.getNextTriggerTime(jobPo.getCronExpression(),
				nextTriggerTime);

		if (nextNextTriggerTime != null && nextTriggerTime != null
				&& (nextNextTriggerTime.getTime() - nextTriggerTime.getTime() >= 3600 * 1000)) {
			return true;
		}

		return false;
	}

	@RequestMapping("job-logger")
	public String jobLoggerUI(Model model, String realTaskId, String taskTrackerNodeGroup, Date startLogTime,
			Date endLogTime) {
		model.addAttribute("realTaskId", realTaskId);
		model.addAttribute("taskTrackerNodeGroup", taskTrackerNodeGroup);
		if (startLogTime == null) {
			startLogTime = DateUtils.addHour(new Date(), -1);
		}

		model.addAttribute("logTypeList", LogType.values());
		model.addAttribute("logLevelList", Level.values());

		model.addAttribute("startLogTime", DateUtils.formatYMD_HMS(startLogTime));
		if (endLogTime == null) {
			endLogTime = new Date();
		}
		model.addAttribute("endLogTime", DateUtils.formatYMD_HMS(endLogTime));
		setAttr(model);
		return "jobLogger";
	}

	@RequestMapping("cron-job-queue")
	public String cronJobQueueUI(Model model) {
		setAttr(model);
		return "cronJobQueue";
	}

	@RequestMapping("repeat-job-queue")
	public String repeatJobQueueUI(Model model) {
		setAttr(model);
		return "repeatJobQueue";
	}

	@RequestMapping("executable-job-queue")
	public String executableJobQueueUI(Model model) {
		setAttr(model);
		return "executableJobQueue";
	}

	@RequestMapping("executing-job-queue")
	public String executingJobQueueUI(Model model) {
		setAttr(model);
		return "executingJobQueue";
	}

	@RequestMapping("load-job")
	public String loadJobUI(Model model) {
		setAttr(model);
		return "loadJob";
	}

	@RequestMapping("cron_generator_iframe")
	public String cronGeneratorIframe(Model model) {
		return "cron/cronGenerator";
	}

	@RequestMapping("suspend-job-queue")
	public String suspendJobQueueUI(Model model) {
		setAttr(model);
		return "suspendJobQueue";
	}

	@RequestMapping("job-gray-list")
	public String jobGrayListUI(Model model) {
		setAttr(model);
		return "jobGrayList";
	}

	@RequestMapping("job-dependency")
	public String jobDependencyUI(Model model) {
		setAttr(model);
		return "jobDependency";
	}

	private void setAttr(Model model) {
		List<NodeGroupPo> jobClientNodeGroups = appContext.getNodeGroupStore().getNodeGroup(NodeType.JOB_CLIENT);
		model.addAttribute("jobClientNodeGroups", jobClientNodeGroups);
		List<NodeGroupPo> taskTrackerNodeGroups = appContext.getNodeGroupStore().getNodeGroup(NodeType.TASK_TRACKER);
		model.addAttribute("taskTrackerNodeGroups", taskTrackerNodeGroups);

		List<String> grayGroups = new ArrayList<String>();
		grayGroups.add("BLUE");
		grayGroups.add("GREEN");
		model.addAttribute("grayGroups", grayGroups);
	}

}
