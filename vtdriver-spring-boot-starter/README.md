### starter的作用
vtdriver-spring-boot-starter目前主要功能：
- 集成VtDriver预热逻辑，在项目启动时,对VtDriver进行预热
- 集成分表配置到spring配置中
### 使用方式
1. 使用vtdriver-spring-boot-starter替换vtdriver的pom依赖
```xml
<dependency>
    <groupId>io.vitess.driver</groupId>
    <artifactId>vtdriver-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```
2. 在springboot中配置分表
```xml
vtdriver.split-table.schemas[0].schema=customer
vtdriver.split-table.schemas[0].logic-tables[0].actual-table-exprs=table_engine_test_${1..4}
vtdriver.split-table.schemas[0].logic-tables[0].logic-table=table_engine_test
vtdriver.split-table.schemas[0].logic-tables[0].sharding-algorithms=ShardTableByString
vtdriver.split-table.schemas[0].logic-tables[0].sharding-column-name=f_key
vtdriver.split-table.schemas[0].logic-tables[0].sharding-column-type=INT32

vtdriver.split-table.schemas[1].schema=commerce
vtdriver.split-table.schemas[1].logic-tables[0].actual-table-exprs=table_engine_test_${1..4}
vtdriver.split-table.schemas[1].logic-tables[0].logic-table=table_engine_test
vtdriver.split-table.schemas[1].logic-tables[0].sharding-algorithms=ShardTableByString
vtdriver.split-table.schemas[1].logic-tables[0].sharding-column-name=f_key
vtdriver.split-table.schemas[1].logic-tables[0].sharding-column-type=INT32
```
或使用yaml：
```yaml
vtdriver:
  split-table:
    schemas:
      - schema: 'customer'
        logic-tables:
          - actualTableExprs: 'table_engine_test_${1..4}'
            logicTable: table_engine_test
            shardingAlgorithms: ShardTableByString
            shardingColumnName: f_key
            shardingColumnType: INT32
      - schema: 'commerce'
        logic-tables:
          - actualTableExprs: 'table_engine_test_${1..4}'
            logicTable: table_engine_test
            shardingAlgorithms: ShardTableByString
            shardingColumnName: f_key
            shardingColumnType: INT32
```