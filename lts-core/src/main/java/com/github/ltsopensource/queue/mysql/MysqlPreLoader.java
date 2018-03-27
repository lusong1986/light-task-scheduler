package com.github.ltsopensource.queue.mysql;

import java.util.List;

import com.github.ltsopensource.core.AppContext;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.core.support.JobQueueUtils;
import com.github.ltsopensource.core.support.SystemClock;
import com.github.ltsopensource.queue.AbstractPreLoader;
import com.github.ltsopensource.queue.domain.JobGrayPo;
import com.github.ltsopensource.queue.domain.JobPo;
import com.github.ltsopensource.queue.mysql.support.RshHolder;
import com.github.ltsopensource.store.jdbc.SqlTemplate;
import com.github.ltsopensource.store.jdbc.SqlTemplateFactory;
import com.github.ltsopensource.store.jdbc.builder.SelectSql;
import com.github.ltsopensource.store.jdbc.builder.UpdateSql;

/**
 * @author Robert HG (254963746@qq.com) on 8/14/15.
 */
public class MysqlPreLoader extends AbstractPreLoader {

	private static final Logger LOGGER = LoggerFactory.getLogger(MysqlPreLoader.class);

	private SqlTemplate sqlTemplate;

	public MysqlPreLoader(AppContext appContext) {
		super(appContext);
		this.sqlTemplate = SqlTemplateFactory.create(appContext.getConfig());
	}

	@Override
	protected JobPo getJob(String taskTrackerNodeGroup, String jobId) {
		return new SelectSql(sqlTemplate).select().all().from().table(getTableName(taskTrackerNodeGroup))
				.where("job_id = ?", jobId).and("task_tracker_node_group = ?", taskTrackerNodeGroup)
				.single(RshHolder.JOB_PO_RSH);
	}

	@Override
	protected boolean lockJob(String taskTrackerNodeGroup, String jobId, String taskTrackerIdentity, Long triggerTime,
			Long gmtModified) {
		try {
			return new UpdateSql(sqlTemplate).update().table(getTableName(taskTrackerNodeGroup))
					.set("is_running", true).set("task_tracker_identity", taskTrackerIdentity)
					.set("gmt_modified", SystemClock.now()).where("job_id = ?", jobId).and("is_running = ?", false)
					.and("trigger_time = ?", triggerTime).and("gmt_modified = ?", gmtModified).doUpdate() == 1;
		} catch (Exception e) {
			LOGGER.error("Error when lock job:" + e.getMessage(), e);
			return false;
		}
	}

	private final String takeSelectSQL = "SELECT *" + " FROM `{tableName}` "
			+ " WHERE is_running = ? AND `trigger_time` < ? "
			+ " AND job_id NOT IN (SELECT DISTINCT job_id FROM lts_executable_job_dependency WHERE status = 1)"
			+ " ORDER BY `priority` ASC, `trigger_time` ASC,  `gmt_created` ASC " + " LIMIT ?, ?";

	@Override
	protected List<JobPo> load(String loadTaskTrackerNodeGroup, int loadSize) {
		try {
			// new SelectSql(sqlTemplate).select().all().from().table(getTableName(loadTaskTrackerNodeGroup))
			// .where("is_running = ?", false).and("trigger_time< ?", SystemClock.now()).orderBy()
			// .column("priority", OrderByType.ASC).column("trigger_time", OrderByType.ASC)
			// .column("gmt_created", OrderByType.ASC).limit(0, loadSize).list(RshHolder.JOB_PO_LIST_RSH);

			return sqlTemplate.query(getRealSql(takeSelectSQL, loadTaskTrackerNodeGroup), RshHolder.JOB_PO_LIST_RSH,
					false, SystemClock.now(), 0, loadSize);
		} catch (Exception e) {
			LOGGER.error("Error when load job:" + e.getMessage(), e);
			return null;
		}
	}

	private String getRealSql(final String sql, final String taskTrackerNodeGroup) {
		String fineSQL = sql.replace("{tableName}", getTableName(taskTrackerNodeGroup));
		return fineSQL;
	}

	private String getTableName(String taskTrackerNodeGroup) {
		return JobQueueUtils.getExecutableQueueName(taskTrackerNodeGroup);
	}

	@Override
	protected JobGrayPo getGrayPo(String taskId) {
		try {
			return new SelectSql(sqlTemplate).select().all().from().table(JobQueueUtils.JOB_GRAY_FLAG)
					.where("task_id = ?", taskId).limit(0, 1).single(RshHolder.JOB_GRAY_PO_RSH);
		} catch (Exception e) {
			LOGGER.error("Error when load job:" + e.getMessage(), e);
			return null;
		}
	}
}
