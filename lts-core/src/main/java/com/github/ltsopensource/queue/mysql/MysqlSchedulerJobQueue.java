package com.github.ltsopensource.queue.mysql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.github.ltsopensource.admin.request.JobQueueReq;
import com.github.ltsopensource.admin.response.PaginationRsp;
import com.github.ltsopensource.core.cluster.Config;
import com.github.ltsopensource.core.commons.utils.CharacterUtils;
import com.github.ltsopensource.core.support.JobQueueUtils;
import com.github.ltsopensource.core.support.SystemClock;
import com.github.ltsopensource.queue.SchedulerJobQueue;
import com.github.ltsopensource.queue.domain.JobGrayPo;
import com.github.ltsopensource.queue.domain.JobPo;
import com.github.ltsopensource.queue.mysql.support.RshHolder;
import com.github.ltsopensource.store.jdbc.builder.OrderByType;
import com.github.ltsopensource.store.jdbc.builder.SelectSql;
import com.github.ltsopensource.store.jdbc.builder.UpdateSql;
import com.github.ltsopensource.store.jdbc.builder.WhereSql;

/**
 * @author Robert HG (254963746@qq.com) on 4/4/16.
 */
public abstract class MysqlSchedulerJobQueue extends AbstractMysqlJobQueue implements SchedulerJobQueue {

	public MysqlSchedulerJobQueue(Config config) {
		super(config);
	}

	@Override
	public boolean updateLastGenerateTriggerTime(String jobId, Long lastGenerateTriggerTime) {
		return new UpdateSql(getSqlTemplate()).update().table(getTableName())
				.set("last_generate_trigger_time", lastGenerateTriggerTime).set("gmt_modified", SystemClock.now())
				.where("job_id = ? ", jobId).doUpdate() == 1;
	}

	@Override
	public List<JobPo> getNeedGenerateJobPos(Long checkTime, int topSize) {
		return new SelectSql(getSqlTemplate()).select().all().from().table(getTableName())
				.where("rely_on_prev_cycle = ?", false).and("last_generate_trigger_time <= ?", checkTime)
				.limit(0, topSize).list(RshHolder.JOB_PO_LIST_RSH);
	}
	
	public PaginationRsp<JobPo> pageSelect(JobQueueReq request) {
		PaginationRsp<JobPo> response = new PaginationRsp<JobPo>();

		WhereSql whereSql = buildWhereSql(request);
		Long results = new SelectSql(getSqlTemplate()).select().columns("count(1)").from().table(getTableName(request))
				.whereSql(whereSql).single();
		response.setResults(results.intValue());

		if (results > 0) {
			List<JobPo> jobPos = new SelectSql(getSqlTemplate())
					.select()
					.all()
					.from()
					.table(getTableName(request))
					.whereSql(whereSql)
					.orderBy()
					.column(CharacterUtils.camelCase2Underscore(request.getField()),
							OrderByType.convert(request.getDirection())).limit(request.getStart(), request.getLimit())
					.list(RshHolder.JOB_PO_LIST_RSH);
			response.setRows(jobPos);

			HashMap<String, JobPo> taskJobPoMap = new HashMap<String, JobPo>();
			List<Object> taskIdList = new ArrayList<Object>();
			for (JobPo jobPo : jobPos) {
				taskIdList.add(jobPo.getTaskId());
				taskJobPoMap.put(jobPo.getTaskId(), jobPo);
			}

			whereSql = new WhereSql().andIn("task_id", taskIdList);
			Long grayTaskResults = new SelectSql(getSqlTemplate()).select().columns("count(1)").from()
					.table(JobQueueUtils.JOB_GRAY_FLAG).whereSql(whereSql).single();
			if (grayTaskResults > 0) {
				List<JobGrayPo> jobGrayPos = new SelectSql(getSqlTemplate()).select().all().from()
						.table(JobQueueUtils.JOB_GRAY_FLAG).whereSql(whereSql).list(RshHolder.JOB_GRAY_PO_LIST_RSH);
				for (JobGrayPo jobGrayPo : jobGrayPos) {
					JobPo jobPo = taskJobPoMap.get(jobGrayPo.getTaskId());
					if (jobPo != null) {
						jobPo.setGray(jobGrayPo.getGray());
					}
				}
			}
		}

		return response;
	}

	protected abstract String getTableName();
}
