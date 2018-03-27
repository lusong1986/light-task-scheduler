package com.github.ltsopensource.queue.mysql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.github.ltsopensource.admin.request.JobQueueReq;
import com.github.ltsopensource.admin.response.PaginationRsp;
import com.github.ltsopensource.core.cluster.Config;
import com.github.ltsopensource.core.cluster.NodeType;
import com.github.ltsopensource.core.commons.utils.CharacterUtils;
import com.github.ltsopensource.core.commons.utils.StringUtils;
import com.github.ltsopensource.core.support.JobQueueUtils;
import com.github.ltsopensource.queue.ExecutableJobDependency;
import com.github.ltsopensource.queue.domain.JobDependencyPo;
import com.github.ltsopensource.queue.domain.JobPo;
import com.github.ltsopensource.queue.domain.NodeGroupPo;
import com.github.ltsopensource.queue.mysql.support.RshHolder;
import com.github.ltsopensource.store.jdbc.JdbcAbstractAccess;
import com.github.ltsopensource.store.jdbc.builder.DeleteSql;
import com.github.ltsopensource.store.jdbc.builder.InsertSql;
import com.github.ltsopensource.store.jdbc.builder.OrderByType;
import com.github.ltsopensource.store.jdbc.builder.SelectSql;
import com.github.ltsopensource.store.jdbc.builder.UpdateSql;
import com.github.ltsopensource.store.jdbc.builder.WhereSql;

public class MysqlExecutableJobDependency extends JdbcAbstractAccess implements ExecutableJobDependency {

	private static final int NO_DEPENDENCY_STATUS = 0;

	private static final int CANNOT_EXECUTE_STATUS = 1;

	private static final int CAN_EXECUTE_STATUS = 2;

	public MysqlExecutableJobDependency(Config config) {
		super(config);
		createTable(readSqlFile("sql/mysql/lts_executable_job_dependency.sql", JobQueueUtils.EXECUTABLE_JOB_DEPENDENCY));
	}

	@Override
	public void add(String jobId, JobDependencyPo jobDependencyPo) {
		Long count = new SelectSql(getSqlTemplate()).select().columns("count(1)").from().table(getTableName())
				.where("job_id = ?", jobId).and("parent_job_id = ?", jobDependencyPo.getParentJobId())
				.and("task_tracker_node_group = ?", jobDependencyPo.getTaskTrackerNodeGroup()).single();
		if (count > 0) {
			return;
		}
		new InsertSql(getSqlTemplate())
				.insert(getTableName())
				.columns("job_id", "parent_job_id", "status", "task_tracker_node_group")
				.values(jobId, jobDependencyPo.getParentJobId(), CANNOT_EXECUTE_STATUS,
						jobDependencyPo.getTaskTrackerNodeGroup()).doInsert();
	}

	@Override
	public void add(JobDependencyPo jobDependencyPo) {
		Long count = new SelectSql(getSqlTemplate()).select().columns("count(1)").from().table(getTableName())
				.where("job_id = ?", jobDependencyPo.getJobId())
				.and("parent_job_id = ?", jobDependencyPo.getParentJobId())
				.and("task_tracker_node_group = ?", jobDependencyPo.getTaskTrackerNodeGroup()).single();
		if (count > 0) {
			return;
		}
		new InsertSql(getSqlTemplate())
				.insert(getTableName())
				.columns("job_id", "parent_job_id", "status", "task_tracker_node_group")
				.values(jobDependencyPo.getJobId(), jobDependencyPo.getParentJobId(), CANNOT_EXECUTE_STATUS,
						jobDependencyPo.getTaskTrackerNodeGroup()).doInsert();
	}

	@Override
	public boolean remove(String jobId) {
		return new DeleteSql(getSqlTemplate()).delete().from().table(getTableName()).where("job_id = ?", jobId)
				.doDelete() == 1;
	}

	@Override
	public boolean removeById(String id) {
		return new DeleteSql(getSqlTemplate()).delete().from().table(getTableName()).where("id = ?", id).doDelete() == 1;
	}

	@Override
	public void add(String jobId) {
		Long count = new SelectSql(getSqlTemplate()).select().columns("count(1)").from().table(getTableName())
				.where("job_id = ?", jobId).and("parent_job_id IS NULL").and("task_tracker_node_group IS NULL")
				.single();
		if (count > 0) {
			// already exist
			return;
		}
		new InsertSql(getSqlTemplate()).insert(getTableName()).columns("job_id").values(jobId).doInsert();
	}

	@Override
	public void removeDependency(String parentJobId, String taskTrackerNodeGroup) {
		new UpdateSql(getSqlTemplate()).update().table(getTableName()).set("parent_job_id", null)
				.set("task_tracker_node_group", null).set("status", NO_DEPENDENCY_STATUS)
				.where("parent_job_id = ?", parentJobId).and("task_tracker_node_group = ?", taskTrackerNodeGroup)
				.doUpdate();
	}

	@Override
	public boolean removeDependency(String id) {
		return new UpdateSql(getSqlTemplate()).update().table(getTableName()).set("parent_job_id", null)
				.set("task_tracker_node_group", null).set("status", NO_DEPENDENCY_STATUS).where("id = ?", id)
				.doUpdate() == 1;
	}

	@Override
	public void updateCanExecuteByParentJob(String parentJobId, String taskTrackerNodeGroup) {
		new UpdateSql(getSqlTemplate()).update().table(getTableName()).set("status", CAN_EXECUTE_STATUS)
				.where("parent_job_id = ?", parentJobId).and("task_tracker_node_group = ?", taskTrackerNodeGroup)
				.and("status = " + CANNOT_EXECUTE_STATUS).doUpdate();
	}

	@Override
	public void updateCanNotExecuteByParentJob(String parentJobId, String taskTrackerNodeGroup) {
		new UpdateSql(getSqlTemplate()).update().table(getTableName()).set("status", CANNOT_EXECUTE_STATUS)
				.where("parent_job_id = ?", parentJobId).and("task_tracker_node_group = ?", taskTrackerNodeGroup)
				.and("status = " + CAN_EXECUTE_STATUS).doUpdate();
	}

	@Override
	public void updateCanNotExecuteBySonJob(String jobId) {
		new UpdateSql(getSqlTemplate()).update().table(getTableName()).set("status", CANNOT_EXECUTE_STATUS)
				.where("job_id = ?", jobId).and("status = " + CAN_EXECUTE_STATUS).doUpdate();
	}

	private String getTableName() {
		return JobQueueUtils.EXECUTABLE_JOB_DEPENDENCY;
	}

	@Override
	public PaginationRsp<JobDependencyPo> pageSelect(JobQueueReq request) {
		PaginationRsp<JobDependencyPo> response = new PaginationRsp<JobDependencyPo>();

		WhereSql whereSql = buildWhereSql(request);

		Long results = new SelectSql(getSqlTemplate()).select().columns("count(1)").from().table(getTableName())
				.whereSql(whereSql).single();
		response.setResults(results.intValue());
		if (results > 0) {
			final List<JobDependencyPo> jobDependencyPos = new SelectSql(getSqlTemplate())
					.select()
					.all()
					.from()
					.table(getTableName())
					.whereSql(whereSql)
					.orderBy()
					.column(CharacterUtils.camelCase2Underscore(request.getField()),
							OrderByType.convert(request.getDirection())).limit(request.getStart(), request.getLimit())
					.list(RshHolder.JOB_DEPENDENCY_PO_LIST_RSH);

			final List<Object> jobIdList = new ArrayList<Object>();
			for (final JobDependencyPo jobDependencyPo : jobDependencyPos) {
				jobIdList.add(jobDependencyPo.getJobId());
				if (StringUtils.hasText(jobDependencyPo.getParentJobId())) {
					jobIdList.add(jobDependencyPo.getParentJobId());
				}
			}

			final HashMap<String, String> jobTaskMap = new HashMap<String, String>();

			// 查cron 表
			whereSql = new WhereSql().andIn("job_id", jobIdList);
			Long jobResults = new SelectSql(getSqlTemplate()).select().columns("count(1)").from()
					.table(JobQueueUtils.CRON_JOB_QUEUE).whereSql(whereSql).single();
			if (jobResults > 0) {
				List<JobPo> jobPos = new SelectSql(getSqlTemplate()).select().all().from()
						.table(JobQueueUtils.CRON_JOB_QUEUE).whereSql(whereSql).list(RshHolder.JOB_PO_LIST_RSH);
				for (JobPo jobPo : jobPos) {
					jobTaskMap.put(jobPo.getJobId(), jobPo.getTaskId());
				}
			}

			// 查repeat 表
			jobResults = new SelectSql(getSqlTemplate()).select().columns("count(1)").from()
					.table(JobQueueUtils.REPEAT_JOB_QUEUE).whereSql(whereSql).single();
			if (jobResults > 0) {
				List<JobPo> jobPos = new SelectSql(getSqlTemplate()).select().all().from()
						.table(JobQueueUtils.REPEAT_JOB_QUEUE).whereSql(whereSql).list(RshHolder.JOB_PO_LIST_RSH);
				for (JobPo jobPo : jobPos) {
					jobTaskMap.put(jobPo.getJobId(), jobPo.getTaskId());
				}
			}

			// 查所有的executable 表
			final List<NodeGroupPo> ttNodeGroups = new SelectSql(getSqlTemplate()).select().all().from()
					.table(JobQueueUtils.NODE_GROUP_STORE).where("node_type = ?", NodeType.TASK_TRACKER.name())
					.list(RshHolder.NODE_GROUP_LIST_RSH);
			for (final NodeGroupPo nodeGroupPo : ttNodeGroups) {
				final String executableTableName = JobQueueUtils.getExecutableQueueName(nodeGroupPo.getName());
				jobResults = new SelectSql(getSqlTemplate()).select().columns("count(1)").from()
						.table(executableTableName).whereSql(whereSql).single();
				if (jobResults > 0) {
					List<JobPo> jobPos = new SelectSql(getSqlTemplate()).select().all().from()
							.table(executableTableName).whereSql(whereSql).list(RshHolder.JOB_PO_LIST_RSH);
					for (JobPo jobPo : jobPos) {
						jobTaskMap.put(jobPo.getJobId(), jobPo.getTaskId());
					}
				}
			}

			for (final JobDependencyPo jobDependencyPo : jobDependencyPos) {
				jobDependencyPo.setTaskId(jobTaskMap.get(jobDependencyPo.getJobId()));
				if (StringUtils.hasText(jobDependencyPo.getParentJobId())) {
					jobDependencyPo.setParentTaskId(jobTaskMap.get(jobDependencyPo.getParentJobId()));
				}
			}

			response.setRows(jobDependencyPos);
		}
		return response;
	}

	private WhereSql buildWhereSql(JobQueueReq request) {
		return new WhereSql().andOnNotEmpty("job_id = ?", request.getJobId()).and("parent_job_id IS NOT NULL")
				.andOnNotEmpty("parent_job_id = ?", request.getParentJobId())
				.andOnNotEmpty("task_tracker_node_group = ?", request.getTaskTrackerNodeGroup());
	}

}
