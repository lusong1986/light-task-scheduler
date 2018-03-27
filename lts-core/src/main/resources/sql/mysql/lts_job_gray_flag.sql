CREATE TABLE IF NOT EXISTS `{tableName}` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `task_id` varchar(64) NOT NULL,
  `gray` varchar(20) NOT NULL DEFAULT '' COMMENT 'green,blue,empty',
  `gmt_created` bigint(20) DEFAULT NULL COMMENT '创建时间',
  `gmt_modified` bigint(20) DEFAULT NULL COMMENT '修改时间',
  PRIMARY KEY (`id`),
  KEY `key_task_id` (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
