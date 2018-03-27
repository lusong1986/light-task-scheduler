CREATE TABLE IF NOT EXISTS `{tableName}` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `job_id` varchar(32) NOT NULL,
  `parent_job_id` varchar(32) DEFAULT NULL,
  `status` tinyint(4) NOT NULL DEFAULT '0' COMMENT '0:normal,1:run,2:complete',
  `task_tracker_node_group` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `key_job_id` (`job_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
