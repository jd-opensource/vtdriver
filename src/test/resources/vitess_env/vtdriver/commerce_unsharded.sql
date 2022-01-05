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
) ENGINE = InnoDB AUTO_INCREMENT = 1 DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `user_unsharded` (
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
CREATE TABLE IF NOT EXISTS `user_unsharded_extra` (
    `id` bigint(20) NOT NULL AUTO_INCREMENT,
    `user_id` bigint(20) NOT NULL,
    `extra_id` bigint(20) NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `music` (
    `id` bigint(20) NOT NULL AUTO_INCREMENT,
    `user_id` bigint(20) NOT NULL,
    `col` varchar(100) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `unsharded_auto` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `val` varchar(100) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `unsharded` (
    `predef1` int(11) NOT NULL,
    `predef2` int(11) DEFAULT NULL,
    PRIMARY KEY (`predef1`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS `unsharded_authoritative` (
    `col1` int(11) DEFAULT NULL,
    `col2` int(11) DEFAULT NULL
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
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
CREATE TABLE IF NOT EXISTS `sbtest1` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `k` int(10) unsigned NOT NULL DEFAULT '0',
  `c` char(120) NOT NULL DEFAULT '',
  `pad` char(60) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  KEY `k` (`k`)
) ENGINE=InnoDB AUTO_INCREMENT=1149567 DEFAULT CHARSET=utf8mb4;