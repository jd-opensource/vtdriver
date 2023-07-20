### Partition Table：

分表是VtDriver开发的一个新功能点，基于现有的分片执行计划/引擎基础开发而来，主要针对单表数据量过大而影响性能的问题。不同于分片功能，所有的表元数据以及分片算法存储于etcd/zookeeper中，分表配置需要使用者在Java项目resources路径下新建vtdriver-split-table.yml文件，文件格式如下：

#### 目前支持的特性：

* 支持单表的增删改查操作，包括聚合查询

* 分表键与分片键无关联，可以是相同字段，也可以是不同字段

* 内置多种分表算法选择，也可以基于SPI拓展

* 支持全局自增ID (Sequence)，自增字段任意，也可以是分表键或者分片键

**暂时还不支持**：

* 流式查询

* Join/Subquery/Union查询

#### 分表配置

```yml
schemas:
  - schema: commerce
    logicTables:
      - { actualTableExprs: 'table_engine_test_${1..4}',
          logicTable: table_engine_test,
          shardingAlgorithms: TableRuleMod,
          shardingColumnName: f_key,
          shardingColumnType: INT32 }

  - schema: customer
    logicTables:
      - { actualTableExprs: 'table_engine_test_${1..4}',
          logicTable: table_engine_test,
          shardingAlgorithms: TableRuleMod,
          shardingColumnName: f_key,
          shardingColumnType: INT32,
          sequenceColumnName: id }
```

* schema: 需要分表的库名

* actualTableExprs: 真实表表达式

* logicTable: 分表的逻辑表，不需要有真实表

* shardingAlgorithms: 分表算法

* shardingColumnName: 分表字段名称

* shardingColumnType: 分表字段类型

* sequenceColumnName: 分表全局自增字段名称 (不使用全局自增时无需配置，需1.2.16版本及以上)

##### 分表sequence配置
1.在分表配置中使用sequenceColumnName指定自增列，示例见以上配置信息；

2.对于每张使用sequence的逻辑表如`table`, 需创建对应的序列表`table_seq`，并插入单条配置数据：

```sql
set @sharding = 'table_seq singleShard'; -- 单分片库不需要此行 
CREATE TABLE `table_seq`
(
    `id`      int(11) NOT NULL COMMENT 'id，设0',
    `next_id` bigint(20) DEFAULT NULL COMMENT '自增id当前边界值，设1；如果想要id从特定值开始自增，则设为相应值即可',
    `cache`   bigint(20) DEFAULT NULL COMMENT 'sequence本地缓存大小，建议值100',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8 COMMENT ='vitess_sequence';

INSERT INTO `table_seq`(id, next_id, cache) values (0, 1, 100);
```

如对于以上示例中的分表配置，逻辑表`table_engine_test`中列`id`使用全局自增，则需要创建对应的序列表`table_engine_test_seq`并插入单条配置数据即可。