CREATE TABLE IF NOT EXISTS `plan_test` (
    `f_tinyint` bigint(20) NOT NULL,
    `f_utinyint` tinyint(3) unsigned DEFAULT NULL,
    `f_smallint` smallint(6) DEFAULT NULL,
    `f_usmallint` smallint(5) unsigned DEFAULT NULL,
    `f_midint` mediumint(9) DEFAULT NULL,
    `f_umidint` mediumint(8) unsigned DEFAULT NULL,
    `f_int` int(11) DEFAULT NULL,
    `f_uint` int(10) unsigned DEFAULT NULL,
    `f_bigint` bigint(20) DEFAULT NULL,
    `f_ubigint` bigint(20) unsigned DEFAULT NULL,
    `f_float` float DEFAULT NULL,
    `f_double` double DEFAULT NULL,
    `f_decimal` decimal(19, 4) DEFAULT NULL,
    `f_date` date DEFAULT NULL,
    `f_time` time DEFAULT NULL,
    `f_datetime` datetime DEFAULT NULL,
    `f_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `f_bit` bit(1) DEFAULT NULL,
    `f_varchar` varchar(255) DEFAULT NULL,
    `f_text` text,
    PRIMARY KEY (`f_tinyint`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8 COMMENT = 'plan test';
CREATE TABLE IF NOT EXISTS `user` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `name` varchar(100) DEFAULT NULL,
    `costly` int(11) DEFAULT NULL,
    `predef1` int(11) DEFAULT NULL,
    `predef2` int(11) DEFAULT NULL,
    `textcol1` varchar(255) DEFAULT NULL,
    `textcol2` varchar(255) DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `user_costly_uindex` (`costly`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `user_extra` (
    `id` bigint(20) NOT NULL AUTO_INCREMENT,
    `user_id` bigint(20) NOT NULL,
    `extra_id` bigint(20) NOT NULL,
    `email` varchar(200) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `music` (
    `id` bigint(20) NOT NULL AUTO_INCREMENT,
    `user_id` bigint(20) NOT NULL,
    `col` varchar(100) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `authoritative` (
    `user_id` int(11) DEFAULT NULL,
    `col1` varchar(100) DEFAULT NULL,
    `col2` int(11) DEFAULT NULL
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `user_metadata` (
    `id` bigint(20) NOT NULL AUTO_INCREMENT,
    `user_id` bigint(20) NOT NULL,
    `email` varchar(100) DEFAULT NULL,
    `address` varchar(500) DEFAULT NULL,
    `md5` varchar(50) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `engine_test` (
    `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
    `f_key` char(32) NOT NULL DEFAULT '' COMMENT '分片键',
    `f_tinyint` tinyint(4) DEFAULT NULL,
    `f_u_tinyint` tinyint(3) unsigned DEFAULT NULL,
    `f_smallint` smallint(6) DEFAULT NULL,
    `f_u_smallint` smallint(5) unsigned DEFAULT NULL,
    `f_mediumint` mediumint(9) DEFAULT NULL,
    `f_u_mediumint` mediumint(8) unsigned DEFAULT NULL,
    `f_int` int(11) DEFAULT NULL,
    `f_u_int` int(10) unsigned DEFAULT NULL,
    `f_bigint` bigint(20) DEFAULT NULL,
    `f_u_bigint` bigint(20) unsigned DEFAULT NULL,
    `f_float` float DEFAULT NULL,
    `f_u_float` float unsigned DEFAULT NULL,
    `f_double` double DEFAULT NULL,
    `f_u_double` double unsigned DEFAULT NULL,
    `f_decimal` decimal(65, 30) DEFAULT NULL,
    `f_u_decimal` decimal(65, 30) unsigned DEFAULT NULL,
    `f_bit` bit(64) DEFAULT NULL,
    `f_date` date DEFAULT NULL,
    `f_time` time DEFAULT NULL,
    `f_datetime` datetime DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
    `f_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `f_boolean` tinyint(1) unsigned DEFAULT NULL,
    `f_varchar` varchar(255) DEFAULT NULL,
    `f_text` text,
    `f_varbinary` varbinary(0) DEFAULT NULL,
    `f_blob` blob,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB AUTO_INCREMENT = 2 DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `table_engine_test_1` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `f_key` char(32) NOT NULL DEFAULT '' COMMENT '分片键',
  `f_tinyint` tinyint(4) DEFAULT NULL,
  `f_bit` bit(1) DEFAULT NULL,
  `f_midint` mediumint(9) DEFAULT NULL,
  `f_int` int(11) DEFAULT NULL,
  `f_varchar` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1000 DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `table_engine_test_2` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `f_key` char(32) NOT NULL DEFAULT '' COMMENT '分片键',
  `f_tinyint` tinyint(4) DEFAULT NULL,
  `f_bit` bit(1) DEFAULT NULL,
  `f_midint` mediumint(9) DEFAULT NULL,
  `f_int` int(11) DEFAULT NULL,
  `f_varchar` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1000 DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `table_engine_test_3` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `f_key` char(32) NOT NULL DEFAULT '' COMMENT '分片键',
  `f_tinyint` tinyint(4) DEFAULT NULL,
  `f_bit` bit(1) DEFAULT NULL,
  `f_midint` mediumint(9) DEFAULT NULL,
  `f_int` int(11) DEFAULT NULL,
  `f_varchar` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 100 DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `table_engine_test_4` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `f_key` char(32) NOT NULL DEFAULT '' COMMENT '分片键',
  `f_tinyint` tinyint(4) DEFAULT NULL,
  `f_bit` bit(1) DEFAULT NULL,
  `f_midint` mediumint(9) DEFAULT NULL,
  `f_int` int(11) DEFAULT NULL,
  `f_varchar` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 301 DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `shard_by_long_test_1` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `f_key` int(10) NOT NULL COMMENT '分片键',
  `f_tinyint` tinyint(3) DEFAULT NULL,
  `f_bit` bit(1) DEFAULT NULL,
  `f_midint` mediumint(7) DEFAULT NULL,
  `f_int` int(10) DEFAULT NULL,
  `f_varchar` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `shard_by_long_test_2` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `f_key` int(10) NOT NULL COMMENT '分片键',
  `f_tinyint` tinyint(3) DEFAULT NULL,
  `f_bit` bit(1) DEFAULT NULL,
  `f_midint` mediumint(7) DEFAULT NULL,
  `f_int` int(10) DEFAULT NULL,
  `f_varchar` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `shard_by_long_test_3` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `f_key` int(10) NOT NULL COMMENT '分片键',
  `f_tinyint` tinyint(3) DEFAULT NULL,
  `f_bit` bit(1) DEFAULT NULL,
  `f_midint` mediumint(7) DEFAULT NULL,
  `f_int` int(10) DEFAULT NULL,
  `f_varchar` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `shard_by_long_test_4` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `f_key` int(10) NOT NULL COMMENT '分片键',
  `f_tinyint` tinyint(3) DEFAULT NULL,
  `f_bit` bit(1) DEFAULT NULL,
  `f_midint` mediumint(7) DEFAULT NULL,
  `f_int` int(10) DEFAULT NULL,
  `f_varchar` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `shard_by_murmur_test_1` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `f_key` char(32) NOT NULL DEFAULT '' COMMENT '分片键',
  `f_tinyint` tinyint(4) DEFAULT NULL,
  `f_bit` bit(1) DEFAULT NULL,
  `f_midint` mediumint(9) DEFAULT NULL,
  `f_int` int(11) DEFAULT NULL,
  `f_varchar` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `shard_by_murmur_test_2` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `f_key` char(32) NOT NULL DEFAULT '' COMMENT '分片键',
  `f_tinyint` tinyint(4) DEFAULT NULL,
  `f_bit` bit(1) DEFAULT NULL,
  `f_midint` mediumint(9) DEFAULT NULL,
  `f_int` int(11) DEFAULT NULL,
  `f_varchar` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `shard_by_murmur_test_3` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `f_key` char(32) NOT NULL DEFAULT '' COMMENT '分片键',
  `f_tinyint` tinyint(4) DEFAULT NULL,
  `f_bit` bit(1) DEFAULT NULL,
  `f_midint` mediumint(9) DEFAULT NULL,
  `f_int` int(11) DEFAULT NULL,
  `f_varchar` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `shard_by_murmur_test_4` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `f_key` char(32) NOT NULL DEFAULT '' COMMENT '分片键',
  `f_tinyint` tinyint(4) DEFAULT NULL,
  `f_bit` bit(1) DEFAULT NULL,
  `f_midint` mediumint(9) DEFAULT NULL,
  `f_int` int(11) DEFAULT NULL,
  `f_varchar` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `shard_rule_mod_1` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `f_key` int(10) NOT NULL COMMENT '分片键',
  `f_tinyint` tinyint(3) DEFAULT NULL,
  `f_bit` bit(1) DEFAULT NULL,
  `f_midint` mediumint(7) DEFAULT NULL,
  `f_int` int(10) DEFAULT NULL,
  `f_varchar` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `shard_rule_mod_2` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `f_key` int(10) NOT NULL COMMENT '分片键',
  `f_tinyint` tinyint(3) DEFAULT NULL,
  `f_bit` bit(1) DEFAULT NULL,
  `f_midint` mediumint(7) DEFAULT NULL,
  `f_int` int(10) DEFAULT NULL,
  `f_varchar` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `shard_rule_mod_3` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `f_key` int(10) NOT NULL COMMENT '分片键',
  `f_tinyint` tinyint(3) DEFAULT NULL,
  `f_bit` bit(1) DEFAULT NULL,
  `f_midint` mediumint(7) DEFAULT NULL,
  `f_int` int(10) DEFAULT NULL,
  `f_varchar` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `shard_rule_mod_4` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `f_key` int(10) NOT NULL COMMENT '分片键',
  `f_tinyint` tinyint(3) DEFAULT NULL,
  `f_bit` bit(1) DEFAULT NULL,
  `f_midint` mediumint(7) DEFAULT NULL,
  `f_int` int(10) DEFAULT NULL,
  `f_varchar` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `table_auto_1` (
  `id` bigint(20) NOT NULL,
  `ai` bigint(20) NOT NULL AUTO_INCREMENT,
  `email` varbinary(128) DEFAULT NULL,
  PRIMARY KEY (`ai`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;
CREATE TABLE IF NOT EXISTS `table_auto_2` (
  `id` bigint(20) NOT NULL,
  `ai` bigint(20) NOT NULL AUTO_INCREMENT,
  `email` varbinary(128) DEFAULT NULL,
  PRIMARY KEY (`ai`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;
CREATE TABLE IF NOT EXISTS `table_auto_3` (
  `id` bigint(20) NOT NULL,
  `ai` bigint(20) NOT NULL AUTO_INCREMENT,
  `email` varbinary(128) DEFAULT NULL,
  PRIMARY KEY (`ai`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;
CREATE TABLE IF NOT EXISTS `table_auto_4` (
  `id` bigint(20) NOT NULL,
  `ai` bigint(20) NOT NULL AUTO_INCREMENT,
  `email` varbinary(128) DEFAULT NULL,
  PRIMARY KEY (`ai`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;
CREATE TABLE IF NOT EXISTS `mds_log` (
  `id` int(11) NOT NULL COMMENT 'id',
  `username` varchar(255) DEFAULT NULL COMMENT '名字',
  `password` varchar(255) DEFAULT NULL COMMENT '密码啊',
  `desc2` varchar(500) DEFAULT NULL COMMENT '描述',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET = utf8 ROW_FORMAT = DYNAMIC;
CREATE TABLE IF NOT EXISTS `mds_login` (
  `id` int(11) NOT NULL COMMENT 'id',
  `username` varchar(255) DEFAULT NULL COMMENT '名字',
  `password` varchar(255) DEFAULT NULL COMMENT '密码啊',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET = utf8 ROW_FORMAT = DYNAMIC;
CREATE TABLE IF NOT EXISTS `mds_users` (
  `id` int(11) unsigned NOT NULL COMMENT 'id',
  `name` varchar(255) DEFAULT NULL COMMENT '名字',
  `age` int(11) DEFAULT NULL COMMENT '年龄',
  `sex` tinyint(4) DEFAULT NULL COMMENT '性别',
  `desc` varchar(500) DEFAULT NULL COMMENT '描述',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `delete_flag` tinyint(4) NOT NULL DEFAULT '0' COMMENT '删除标识',
  PRIMARY KEY (`id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMMENT = '用户信息表';
CREATE TABLE IF NOT EXISTS `mds_user_login` (
  `id` int(11) NOT NULL COMMENT 'id',
  `username` varchar(255) DEFAULT NULL COMMENT '名字',
  `password` varchar(255) DEFAULT NULL COMMENT '密码啊',
  `desc` varchar(500) DEFAULT NULL COMMENT '描述',
  `a` int(11) DEFAULT '10',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET = utf8 ROW_FORMAT = DYNAMIC;
CREATE TABLE IF NOT EXISTS `test` (
  `f_tinyint` tinyint(3) NOT NULL,
  `f_utinyint` tinyint(3) unsigned DEFAULT NULL,
  `f_smallint` smallint(6) DEFAULT NULL,
  `f_usmallint` smallint(5) unsigned DEFAULT NULL,
  `f_midint` mediumint(9) DEFAULT NULL,
  `f_umidint` mediumint(8) unsigned DEFAULT NULL,
  `f_int` int(11) DEFAULT NULL,
  `f_uint` int(10) unsigned DEFAULT NULL,
  `f_bigint` bigint(20) DEFAULT NULL,
  `f_ubigint` bigint(20) unsigned DEFAULT NULL,
  `f_float` float DEFAULT NULL,
  `f_double` double DEFAULT NULL,
  `f_decimal` decimal(19,4) DEFAULT NULL,
  `f_date` date DEFAULT NULL,
  `f_time` time DEFAULT NULL,
  `f_datetime` datetime(3) DEFAULT NULL,
  `f_timestamp` timestamp(3) NULL DEFAULT NULL,
  `f_bit` bit(64) DEFAULT NULL,
  `f_varchar` varchar(255) DEFAULT NULL,
  `f_text` text,
  `f_varbinary` varbinary(1024) DEFAULT NULL,
  `f_blob` blob,
  PRIMARY KEY (`f_tinyint`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='todo';
CREATE TABLE IF NOT EXISTS `auto` (
  `id` bigint(20) NOT NULL,
  `ai` bigint(20) NOT NULL AUTO_INCREMENT,
  `email` varbinary(128) DEFAULT NULL,
  PRIMARY KEY (`ai`)
) ENGINE = InnoDB AUTO_INCREMENT = 1000 DEFAULT CHARSET = utf8;
CREATE TABLE IF NOT EXISTS `time_test` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `date` date DEFAULT NULL,
  `year` year(4) DEFAULT NULL,
  `time0` time DEFAULT NULL,
  `time1` time(1) DEFAULT NULL,
  `time2` time(2) DEFAULT NULL,
  `time3` time(3) DEFAULT NULL,
  `time4` time(4) DEFAULT NULL,
  `time5` time(5) DEFAULT NULL,
  `time6` time(6) DEFAULT NULL,
  `datetime0` datetime DEFAULT NULL,
  `datetime1` datetime(1) DEFAULT NULL,
  `datetime2` datetime(2) DEFAULT NULL,
  `datetime3` datetime(3) DEFAULT NULL,
  `datetime4` datetime(4) DEFAULT NULL,
  `datetime5` datetime(5) DEFAULT NULL,
  `datetime6` datetime(6) DEFAULT NULL,
  `timestamp0` timestamp NULL DEFAULT NULL,
  `timestamp1` timestamp(1) NULL DEFAULT NULL,
  `timestamp2` timestamp(2) NULL DEFAULT NULL,
  `timestamp3` timestamp(3) NULL DEFAULT NULL,
  `timestamp4` timestamp(4) NULL DEFAULT NULL,
  `timestamp5` timestamp(5) NULL DEFAULT NULL,
  `timestamp6` timestamp(6) NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 5001 DEFAULT CHARSET = latin1;
CREATE TABLE IF NOT EXISTS `type_bit_tinyint` (
  `id` int(11) NOT NULL,
  `f_bit_1` bit(1) DEFAULT NULL,
  `f_bit_64` bit(64) DEFAULT NULL,
  `f_u_tinyint_1` tinyint(1) unsigned DEFAULT NULL,
  `f_tinyint_1` tinyint(1) DEFAULT NULL,
  `f_u_tinyint_64` tinyint(128) unsigned DEFAULT NULL,
  `f_tinyint_64` tinyint(128) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `type_test` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `f_key` char(32) NOT NULL DEFAULT '' COMMENT '分片键',
  `f_tinyint` tinyint(4) DEFAULT NULL,
  `f_u_tinyint` tinyint(3) unsigned DEFAULT NULL,
  `f_smallint` smallint(6) DEFAULT NULL,
  `f_u_smallint` smallint(5) unsigned DEFAULT NULL,
  `f_mediumint` mediumint(9) DEFAULT NULL,
  `f_u_mediumint` mediumint(8) unsigned DEFAULT NULL,
  `f_int` int(11) DEFAULT NULL,
  `f_u_int` int(10) unsigned DEFAULT NULL,
  `f_bigint` bigint(20) DEFAULT NULL,
  `f_u_bigint` bigint(20) unsigned DEFAULT NULL,
  `f_float` float DEFAULT NULL,
  `f_u_float` float unsigned DEFAULT NULL,
  `f_double` double DEFAULT NULL,
  `f_u_double` double unsigned DEFAULT NULL,
  `f_decimal` decimal(65, 4) NOT NULL,
  `f_u_decimal` decimal(65, 4) unsigned DEFAULT NULL,
  `f_bit` bit(64) DEFAULT NULL,
  `f_date` date DEFAULT NULL,
  `f_time` time DEFAULT NULL,
  `f_datetime` datetime(3) DEFAULT NULL,
  `f_timestamp` timestamp(3) NULL DEFAULT NULL,
  `f_year` year(4) DEFAULT NULL,
  `f_boolean` tinyint(1) unsigned DEFAULT NULL,
  `f_varchar` varchar(255) DEFAULT NULL,
  `f_text` text,
  `f_ttext` tinytext,
  `f_mtext` mediumtext,
  `f_ltext` longtext,
  `f_varbinary` varbinary(1000) DEFAULT NULL,
  `f_blob` blob,
  `f_mblob` mediumblob,
  `f_tblob` tinyblob,
  `f_lblob` longblob,
  `f_binary` binary(50) DEFAULT NULL,
  `f_enum` enum('Y', 'N') DEFAULT NULL,
  `f_set`
  set('Value A', 'Value B') DEFAULT NULL,
    `f_ger` geometry DEFAULT NULL,
    `f_json` json DEFAULT NULL,
    PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 127 DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `engine_test` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `f_key` char(32) NOT NULL DEFAULT '' COMMENT '分片键',
  `f_tinyint` tinyint(4) DEFAULT NULL,
  `f_u_tinyint` tinyint(3) unsigned DEFAULT NULL,
  `f_smallint` smallint(6) DEFAULT NULL,
  `f_u_smallint` smallint(5) unsigned DEFAULT NULL,
  `f_mediumint` mediumint(9) DEFAULT NULL,
  `f_u_mediumint` mediumint(8) unsigned DEFAULT NULL,
  `f_int` int(11) DEFAULT NULL,
  `f_u_int` int(10) unsigned DEFAULT NULL,
  `f_bigint` bigint(20) DEFAULT NULL,
  `f_u_bigint` bigint(20) unsigned DEFAULT NULL,
  `f_float` float DEFAULT NULL,
  `f_u_float` float unsigned DEFAULT NULL,
  `f_double` double DEFAULT NULL,
  `f_u_double` double unsigned DEFAULT NULL,
  `f_decimal` decimal(65,30) DEFAULT NULL,
  `f_u_decimal` decimal(65,30) unsigned DEFAULT NULL,
  `f_bit` bit(64) DEFAULT NULL,
  `f_date` date DEFAULT NULL,
  `f_time` time DEFAULT NULL,
  `f_datetime` datetime DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  `f_timestamp` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP,
  `f_boolean` tinyint(1) unsigned DEFAULT NULL,
  `f_varchar` varchar(255) DEFAULT NULL,
  `f_text` text,
  `f_varbinary` varbinary(0) DEFAULT NULL,
  `f_blob` blob,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=123456790 DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS `sbtest1` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `k` int(10) unsigned NOT NULL DEFAULT '0',
  `c` char(120) NOT NULL DEFAULT '',
  `pad` char(60) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  KEY `k` (`k`)
) ENGINE=InnoDB AUTO_INCREMENT=1149567 DEFAULT CHARSET=utf8mb4;
