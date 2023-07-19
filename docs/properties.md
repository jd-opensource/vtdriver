### VtDriver支持配置的参数

通过jdbcurl方式传入

##### 1.VtDriver特有参数

| 属性 | 数据类型 | 默认值 | 备注 |
|---|---|---|---|
| cell | String |  | 应用接入时,传入的cell需考虑跨机房切换,传入多个 |
| deepPaginationThreshold | int | 1000000000 | 用来设置深度分页优化的临界值，超过此参数大小会开启深度分页优化 |
| role | String | rw | 用来配置读写分离，默认role=rw，role=rr时只读 |
| role | String |  | role=rr时优先读取replica节点，role=ro时读取rdonly节点 |
| vtPlanCacheCapacity  | int | 300 | 该参数用来设置执行计划缓存cache大小，最大值10240 |
| queryConsolidator | boolean | false | 用来开启Consolidator，仅在role=rr场景生效；相同的sql语句只执行一次，其余线程等待第一次查询返回结果后返回 |
| queryParallelNum | int | 1 | 在分表场景下，执行事务外的SQL语句时每个分片上可开启的最大并发数 |

##### 2.支持MySQL驱动参数

| 属性 | 数据类型 | 默认值 | 备注 |
|---|---|---|---|
| user | String |  | 连接的用户 |
| password | String |  |  连接时使用的密码。 |
| characterEncoding | String | utf8 | 是指定所处理字符的解码和编码的格式，或者说是标准。若项目的字符集和MySQL数据库字符集设置为同一字符集则url可以不加此参数。 |
| serverTimezone | String |  | 设置时区 |
| socketTimeout | int | 10000 | 查询超时时间,最小值不得小于1000，小于1000时默认设置为1000 |
| allowMultiQueries| boolean| true| 在一条语句中，允许使用“;”来分隔多条查询。不可更改，VtDriver强制设置为true|
| maxAllowedPacket | byte | 65535（64k） | 设置server接受的数据包的大小 |
| zeroDateTimeBehavior | String | exception | JAVA连接MySQL数据库，在操作值为0的timestamp类型时不能正确的处理，而是默认抛出一个异常。参数，exception：默认值；convertToNull：将日期转换成NULL值；round：替换成最近的日期 |
| sendFractionalSeconds | boolean | true | 指定发送TIMESTAMPSCHEMA的小数秒部分。若该属性为false，则TIMESTAMP的纳秒部分在发送给服务器之前会被截断。该属性适用于预处理语句，可调用的语句，或可更新的结果集 |
| treatUtilDateAsTimestamp | boolean | true | 为了PreparedStatement.setObject（），驱动程序是否将java.util.Date视为TIMESTAMP |
| useStreamLengthsInPrepStmts | boolean | true | 是否使用PreparedStatement/ResultSet.setXXXStream()方法调用中的流长度参数？true/false，默认为“true”。 |
| autoClosePStmtStreams | boolean | false | 驱动程序是否自动调用.close（）在流/读取器作为参数传递通过set *（）方法？|
| autoReconnect | boolean |false |驱动程序是否应尝试再次建立失效的链接或死连接？ |
| connectTimeout | long | 0 | 套接字连接的超时（单位为毫秒），0表示无超时 |
| useSSL | boolean | false | 在与服务器通信时使用SSL |
| useAffectedRows | boolean | false | 当连接到服务器时不要设置“client_found_rows”标签 （这个是不符合JDBC标准的，它会破坏大部分依赖“found”VS   DML语句下的”affected”应用程序）。但是会导致“insert”里面的“Correct”更新数据。服务器会返回“ON Duplicate Key   update”的状态 |
| rewriteBatchedStatements | boolean | false | 针对Statement.executeBatch(), 是否使用MultiQuery方式执行。在分表场景下，由于分表底层已开启MultiQuery，不能开启这个参数 |

##### 3.线程池参数（内部线程池仅以第一次创建Connection的参数为准）

| 属性 | 数据类型 | 默认值 | 备注 |
|---|---|---|---|
| daemonCoreSize | int | 10 | Daemon线程池核心线程数 |
| daemonMaximumSize | int | 100 | Daemon线程池最大线程数 |
| daemonRejectedTimeout | long | 3000 | Daemon线程池拒绝任务丢弃超时（毫秒） |
| queryCoreSize | int | 当前cpu核心线程数 | 执行SQL线程池核心线程数 |
| queryMaximumSize | int | 100 | 执行SQL线程池最大线程数 |
| queryQueueSize | int | 1000 | 执行SQL线程池任务队列长度 |
| queryRejectedTimeout | long | 3000 | 执行SQL线程池拒绝任务丢弃超时（毫秒） |
| healthCheckCoreSize | int | 10 | healthCheck线程池核心线程数 |
| healthCheckMaximumSize | int | 100 | healthCheck线程池最大线程数 |
| healthCheckQueueSize | int | 10000 | healthCheck线程池任务队列长度 |
| healthCheckRejectedTimeout | long | 3000 | healthCheck线程池拒绝任务丢弃超时（毫秒） |

##### 4.内部连接池（Connection Pool）参数

| 属性 | 数据类型 | 默认值 | 备注 |
|---|---|---|---|
| vtConnectionInitSql | String | select 1 | 此属性设置一个SQL语句，该语句将在每次创建新连接之后执行，然后再将其添加到池中。如果此SQL无效或引发异常，则将其视为连接失败，并将遵循标准重试逻辑。 |
| vtConnectionTestQuery | String | 空 | 如果您的驱动程序支持JDBC4，我们强烈建议您不要设置此属性。这适用于不支持JDBC4的“遗留”驱动程序Connection.isValid() API。这是在从池中给出连接之前执行的查询，以验证与数据库的连接是否仍然存在。再次尝试运行没有此属性的池，如果您的驱动程序不符合JDBC4，将记录错误以通知您。 |
| vtConnectionTimeout | long | 30000 | 此属性控制客户端等待池中连接的最大毫秒数。如果在没有连接可用的情况下超过此时间，则将抛出SQLException。最低可接受的连接超时为250毫秒。 |
| vtIdleTimeout | long | 600000(10分钟) | 此属性控制允许连接在池中处于空闲状态的最长时间。（此设置仅在大于minimumIdle（最小空闲数）小于maximumPoolSize（池大小）时才适用。一旦池到达连接， 空闲连接将不会退出minimumIdle。连接是否空闲退出的最大变化为+30秒，平均变化为+15秒。在此超时之前，连接永远不会被空闲。）值为0表示永远不会从池中删除空闲连接。允许的最小值为10000毫秒（10秒）。 |
| vtMaximumPoolSize | int | 10 |  最大连接数 |
| vtMaxLifetime | long | 10800000(3小时) | 此属性控制池中连接的最长生命周期。使用中的连接永远不会退役。 强烈建议您设置此值，它应比任何数据库或基础结构强加的连接时间限制短几秒。 值0表示没有最大寿命（无限寿命）。 如果设置成0会出现问题：长时间没对库进行操作（超过8小时）连接会依然在池内， 超过8小时数据库会断开与客户端的链接，在时用此链接必定报错 如果不等于0且小于30秒则会被重置回30分钟。 |
| vtMinimumIdle | int | 5 | 最小连接数（初始连接数） |
| vtValidationTimeout | long | 5000 | 此属性控制连接测试活动的最长时间。该值必须小于connectionTimeout。最低可接受的验证超时为250毫秒。 |

*注*：`vtMinimumIdle`和`vtMaximumPoolSize`两个参数同时未指定且分片数大于等于8时, 默认值有所不同:

- 分片数>=8且<16时, `vtMinimumIdle` / `vtMaximumPoolSize` = 4 / 8。
- 分片数>=16且<32时, `vtMinimumIdle` / `vtMaximumPoolSize` = 3 / 6。
- 分片数>=32且<64时, `vtMinimumIdle` / `vtMaximumPoolSize` = 2 / 5。
- 分片数>=64时, `vtMinimumIdle` / `vtMaximumPoolSize` = 2 / 4。

### VtDriver中的系统参数
通过JVM参数方式传入，比如-Dvtdriver.api.port=9999

| 属性 | 数据类型 | 默认值 | 备注 |
|---|---|---|---|
| vtdriver.api.port | int | 15002 | 指定开启的http端口 |
| vtdriver.monitor.port | int | 15001 | 指定开启的http端口(prometheus) |
| vtdriver.secondsBehindMaster | int | 7200 | 指定HealthCheck中判定tablet为可用的最大主从延迟，默认值为7200s |