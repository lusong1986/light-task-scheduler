package com.github.ltsopensource.queue.mysql;

import java.util.List;

import com.github.ltsopensource.admin.request.JobQueueReq;
import com.github.ltsopensource.admin.response.PaginationRsp;
import com.github.ltsopensource.core.cluster.Config;
import com.github.ltsopensource.core.commons.utils.CharacterUtils;
import com.github.ltsopensource.core.support.JobQueueUtils;
import com.github.ltsopensource.core.support.SystemClock;
import com.github.ltsopensource.queue.JobGrayFlag;
import com.github.ltsopensource.queue.domain.JobGrayPo;
import com.github.ltsopensource.queue.mysql.support.RshHolder;
import com.github.ltsopensource.store.jdbc.JdbcAbstractAccess;
import com.github.ltsopensource.store.jdbc.builder.DeleteSql;
import com.github.ltsopensource.store.jdbc.builder.InsertSql;
import com.github.ltsopensource.store.jdbc.builder.OrderByType;
import com.github.ltsopensource.store.jdbc.builder.SelectSql;
import com.github.ltsopensource.store.jdbc.builder.UpdateSql;
import com.github.ltsopensource.store.jdbc.builder.WhereSql;

public class MysqlJobGrayFlag extends JdbcAbstractAccess implements JobGrayFlag {

	public MysqlJobGrayFlag(Config config) {
		super(config);
		createTable(readSqlFile("sql/mysql/lts_job_gray_flag.sql", JobQueueUtils.JOB_GRAY_FLAG));
	}

	@Override
	public void add(String taskId, String gray) {
		Long count = new SelectSql(getSqlTemplate()).select().columns("count(1)").from().table(getTableName())
				.where("task_id = ?", taskId).single();
		if (count > 0) {
			// already exist
			return;
		}

		final long now = SystemClock.now();
		new InsertSql(getSqlTemplate()).insert(getTableName())
				.columns("task_id", "gray", "gmt_created", "gmt_modified").values(taskId, gray, now, now).doInsert();
	}

	@Override
	public void update(String taskId, String gray) {
		Long count = new SelectSql(getSqlTemplate()).select().columns("count(1)").from().table(getTableName())
				.where("task_id = ?", taskId).single();
		if (count > 0) {
			// already exist
			final long now = SystemClock.now();
			new UpdateSql(getSqlTemplate()).update().table(getTableName()).set("gray", gray).set("gmt_modified", now)
					.where("task_id = ?", taskId).doUpdate();
			return;
		}

		final long now = SystemClock.now();
		new InsertSql(getSqlTemplate()).insert(getTableName())
				.columns("task_id", "gray", "gmt_created", "gmt_modified").values(taskId, gray, now, now).doInsert();

	}

	@Override
	public boolean remove(String taskId) {
		return new DeleteSql(getSqlTemplate()).delete().from().table(getTableName()).where("task_id = ?", taskId)
				.doDelete() == 1;
	}

	@Override
	public boolean removeById(String id) {
		return new DeleteSql(getSqlTemplate()).delete().from().table(getTableName()).where("id = ?", id).doDelete() == 1;
	}

	@Override
	public PaginationRsp<JobGrayPo> pageSelect(JobQueueReq request) {
		PaginationRsp<JobGrayPo> response = new PaginationRsp<JobGrayPo>();

		WhereSql whereSql = buildWhereSql(request);

		Long results = new SelectSql(getSqlTemplate()).select().columns("count(1)").from().table(getTableName())
				.whereSql(whereSql).single();
		response.setResults(results.intValue());
		if (results > 0) {
			List<JobGrayPo> jobGrayPos = new SelectSql(getSqlTemplate())
					.select()
					.all()
					.from()
					.table(getTableName())
					.whereSql(whereSql)
					.orderBy()
					.column(CharacterUtils.camelCase2Underscore(request.getField()),
							OrderByType.convert(request.getDirection())).limit(request.getStart(), request.getLimit())
					.list(RshHolder.JOB_GRAY_PO_LIST_RSH);
			response.setRows(jobGrayPos);
		}
		return response;
	}

	private WhereSql buildWhereSql(JobQueueReq request) {
		return new WhereSql().andOnNotEmpty("task_id = ?", request.getTaskId()).andOnNotEmpty("gray = ?",
				request.getGray());
	}

	private String getTableName() {
		return JobQueueUtils.JOB_GRAY_FLAG;
	}

}
