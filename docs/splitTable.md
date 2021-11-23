### Partition Table：

分表是VtDriver开发的一个新功能点，基于现有的分片执行计划/引擎基础开发而来，主要针对单表数据量过大而影响性能的问题。不同于分片功能，所有的表元数据以及分片算法存储于etcd/zookeeper中，分表配置需要使用者在Java项目resources路径下新建vtdriver-split-table.yml文件，文件格式如下：

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
          shardingColumnType: INT32 }
```

* schema: 需要分表的库名

* actualTableExprs: 真实表表达式

* logicTable: 分表的逻辑表，不需要有真实表

* shardingAlgorithms: 分表算法

* shardingColumnName: 分表字段名称

* shardingColumnType: 分表字段类型

目前支持的特性：

* 支持单表的增删改查操作，包括聚合查询

* 分表键与分片键无关联，可以是相同字段，也可以是不同字段

* 内置多种分表算法选择，也可以基于SPI拓展

暂时还不支持：

* 流式查询

* Join/Subquery/Union查询

* Sequence