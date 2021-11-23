
## VtDriver 支持的SQL (2021.11.04)

### 目录

- [简单查询](#1)
- [聚合查询](#2)
- [过滤条件](#3)
- [FROM 子句](#4)
- [排序](#5)
- [Post Process](#6)
- [流式聚合查询](#7)
- [流式排序查询](#8)
- [算术运算](#9)
- [Deep Pagination](#10)
- [JOIN 子句(分片、不分片)](#11)
- [子查询 (分片、不分片)](#12)
- [UNION (分片、不分片)](#13)
- [DML DELETE (分片、不分片)](#14)
- [DML INSERT (分片、不分片)](#15)
- [DML UPDATE (分片、不分片)](#16)

---

### <p id='1'>简单查询</p>

```sql
--- select_cases.json
-- No column referenced
select 1 from plan_test


-- unqualified '*' expression for simple route
select * from plan_test

-- qualified '*' expression for simple route
select plan_test.* from plan_test

-- fully qualified '*' expression for simple route
select :ks.plan_test.* from :ks.plan_test

-- Hex number is not treated as a simple value
select * from plan_test where f_tinyint = 0x04

-- sharded limit offset
select f_int from plan_test order by f_int limit 10, 20

-- Multiple parenthesized expressions
select * from plan_test where (f_tinyint = 4) AND (f_varchar ='abc') limit 5

-- Column Aliasing with Column
select user0_.f_int as col0_ from plan_test user0_ where f_tinyint = 1 order by col0_ desc limit 3

-- Booleans and parenthesis
select * from plan_test where (f_tinyint = 1) AND f_bit = true limit 5

-- Column as boolean-ish
select * from plan_test where (f_tinyint = 1) AND f_bit limit 5

-- PK as fake boolean, and column as boolean-ish
select * from plan_test where (f_tinyint = 5) AND f_bit = true limit 5

-- sql_calc_found_rows without limit
select sql_calc_found_rows * from plan_test where f_tinyint = 1
```

### <p id='2'>聚合查询</p>

```sql
--- aggr_cases.json
-- Aggregate on unique sharded
select count(*), f_int from plan_test where f_tinyint = 1


-- Aggregate detection (non-aggregate function)
select trim(' ABC '), f_tinyint from plan_test

-- select distinct with unique vindex for scatter route.
select distinct f_int, f_tinyint from plan_test

-- distinct and group by together for single route.
select distinct f_int, f_tinyint from plan_test group by f_int

-- count aggregate
select count(*) from plan_test

-- sum aggregate
select sum(f_int) from plan_test

-- min aggregate
select min(f_int) from plan_test

-- max aggregate
select max(f_int) from plan_test

-- distinct and group by together for scatter route
select distinct f_int, f_midint from plan_test group by f_int

-- group by a unique vindex should use a simple route
select f_tinyint, count(*) from plan_test group by f_tinyint

-- group by a unique vindex and other column should use a simple route
select f_tinyint, f_int, count(*) from plan_test group by f_tinyint, f_int

-- group by a non-vindex column should use an OrderdAggregate primitive
select f_int, count(*) from plan_test group by f_int

-- group by a unique vindex should use a simple route, even if aggr is complex
select f_tinyint, 1+count(*) from plan_test group by f_tinyint

-- group by a unique vindex where alias from select list is used
select f_tinyint as val, 1+count(*) from plan_test group by val

-- group by a unique vindex where expression is qualified (alias should be ignored)
select f_int as id, 1+count(*) from plan_test group by plan_test.f_tinyint

-- group by a unique vindex where it should skip non-aliased expressions.
select *, f_tinyint, 1+count(*) from plan_test group by f_tinyint

-- group by a unique vindex should revert to simple route, and having clause should find the correct symbols.
select f_tinyint, count(*) c from plan_test group by f_tinyint having f_tinyint = 1 and c = 1

-- scatter aggregate using distinct
select distinct f_int from plan_test

-- scatter aggregate group by select col
select f_int from plan_test group by f_int

-- count with distinct group by unique vindex
select f_tinyint, count(distinct f_int) from plan_test group by f_tinyint

-- count with distinct unique vindex
select f_int, count(distinct f_tinyint) from plan_test group by f_int

-- count with distinct no unique vindex
select f_int, count(distinct f_timestamp) from plan_test group by f_int

-- count with distinct no unique vindex and no group by
select count(distinct f_int) from plan_test

-- count with distinct no unique vindex, count expression aliased
select f_smallint, count(distinct f_int) c2 from plan_test group by f_smallint

-- sum with distinct no unique vindex
select f_smallint, sum(distinct f_int) from plan_test group by f_smallint

-- min with distinct no unique vindex. distinct is ignored.
select f_smallint, min(distinct f_int) from plan_test group by f_smallint

-- order by count distinct
select f_smallint, count(distinct f_int) k from plan_test group by f_smallint order by k

-- scatter aggregate multiple group by (columns)
select f_smallint, f_int, count(*) from plan_test group by f_int, f_smallint

-- scatter aggregate group by column number
select f_int from plan_test group by 1

-- scatter aggregate order by null
select count(*) from plan_test order by null

-- scatter aggregate with numbered order by columns
select f_smallint, f_midint, f_int, f_varchar, count(*) from plan_test group by 1, 2, 3 order by 1, 2, 3

-- scatter aggregate with named order by columns
select f_smallint, f_midint, f_int, f_varchar, count(*) from plan_test group by 1, 2, 3 order by f_smallint, f_midint, f_int

-- scatter aggregate with jumbled order by columns
select f_smallint, f_midint, f_int, f_bigint, count(*) from plan_test group by 1, 2, 3, 4 order by f_smallint, f_midint, f_int, f_bigint

-- scatter aggregate with jumbled group by and order by columns
select f_smallint, f_midint, f_int, f_bigint, count(*) from plan_test group by 3, 2, 1, 4 order by f_bigint, f_midint, f_smallint, f_int

-- scatter aggregate with some descending order by cols
select f_smallint, f_midint, f_int, count(*) from plan_test group by 3, 2, 1 order by 1 desc, 3 desc, f_midint

-- Group by with collate operator
select plan_test.f_int as a from plan_test where plan_test.f_tinyint = 5 group by a collate utf8_general_ci
```

### <p id='3'>过滤条件</p>

```sql
--- filter_cases.json
-- No where clause
select f_tinyint from plan_test


-- Query that always return empty
select f_tinyint from plan_test where f_umidint = null

-- Single table unique vindex route
select f_tinyint from plan_test where plan_test.f_tinyint = 5

-- Single table unique vindex route, but complex expr
select f_tinyint from plan_test where plan_test.f_tinyint = 5+5

-- Single table complex in clause
select f_tinyint from plan_test where f_varchar in (f_text, 'bb')

-- Route with multiple route constraints, SelectIN is the best constraint
select f_tinyint from plan_test where plan_test.f_midint = 123456 and plan_test.f_tinyint in (1, 2)

-- Route with multiple route constraints and boolean, SelectIN is the best constraint.
select f_tinyint from plan_test where plan_test.f_varchar = case plan_test.f_varchar when 'foo' then true else false end and plan_test.f_tinyint in (1, 2)

-- Route with multiple route constraints, SelectEqualUnique is the best constraint, order reversed.
select f_tinyint from plan_test where plan_test.f_midint = 123456 and plan_test.f_tinyint in (1, 11) and plan_test.f_varchar = 'abc' and plan_test.f_tinyint = 1

-- Route with OR and AND clause, must parenthesize correctly. order needed; if not ordered, it may report fail
select f_tinyint from plan_test where plan_test.f_tinyint = 1 or plan_test.f_varchar = 'abc' and plan_test.f_tinyint in (1, 2, 8)

-- Route with OR and AND clause, must parenthesize correctly.
select f_tinyint from plan_test where plan_test.f_tinyint = 1 or plan_test.f_varchar = 'abc' and plan_test.f_tinyint in (1, 2)

-- SELECT with IS NULL.
select f_tinyint from plan_test where f_umidint is null

-- Single table with unique vindex match and null match.
select f_tinyint from plan_test where f_tinyint = 4 and f_umidint is null

-- Single table with unique vindex match and IN (null).
select f_tinyint from plan_test where f_midint = 123456 and f_tinyint IN (null)

-- Single table with unique vindex match and IN (null, 1, 2).
select f_tinyint from plan_test where f_midint = 123456 and f_tinyint IN (null, 1, 2)

-- Single table with unique vindex match and IN (null, 1, 2).
select f_tinyint from plan_test where f_midint = 123456 and f_tinyint NOT IN (null, 1, 2)
```


### <p id='4'>FROM 子句</p>

```sql
--- from_cases.json
-- Single information_schema query
select table_schema from information_schema.tables


-- access to unqualified column names in information_schema
select table_schema from information_schema.tables where table_schema = 'mysql'

-- access to qualified column names in information_schema
select table_schema from information_schema.tables where information_schema.tables.table_schema = 'mysql'

-- Single performance_schema query
select host from performance_schema.hosts where host in ('localhost', '127.0.0.1')

-- access to unqualified column names in performance_schema
select host from performance_schema.hosts where host = 'localhost'

-- access to qualified column names in performance_schema
select host from performance_schema.hosts where performance_schema.hosts.host = 'localhost'

-- Single sys query
select mysql_version from sys.version

-- access to unqualified column names in sys
select mysql_version from sys.version where mysql_version is not null

-- access to qualified column names in sys
select mysql_version from sys.version where sys.version.mysql_version is not null

-- Single mysql query
select plugin from mysql.user

-- access to unqualified column names in mysql
select plugin from mysql.user where user = 'root' limit 1

-- access to qualified column names in mysql
select plugin from mysql.user where mysql.user.user = 'root' limit 1
```

### <p id='5'>排序</p>

```sql
--- memory_sort_cases.json
-- scatter aggregate order by references ungrouped column
select f_int, f_smallint, count(*) from plan_test group by f_int order by f_smallint


-- scatter aggregate order by references ungrouped column
select f_int, f_smallint, count(*) k from plan_test group by f_int order by k

-- scatter aggregate order by references multiple non-group-by expressions
select f_int, f_smallint, count(*) k from plan_test group by f_int order by f_smallint, f_int, k

-- scatter aggregate with memory sort and limit
select f_int, f_smallint, count(*) k from plan_test group by f_int order by k desc limit 10

-- scatter aggregate with memory sort and order by number
select f_int, f_smallint, count(*) k from plan_test group by f_int order by 1,3
```

### <p id='6'>Post Process</p>

```sql
--- postprocess_cases.json
-- ORDER BY uses column numbers
select f_midint from plan_test where f_tinyint = 1 order by 1


-- ORDER BY uses column numbers
select f_midint from plan_test order by f_midint

-- ORDER BY on scatter with multiple number column
select f_smallint, f_midint, f_int from plan_test order by f_smallint, f_midint, f_int

-- ORDER BY on scatter with number column, qualified name
select f_smallint, plan_test.f_midint, f_int from plan_test order by f_smallint, f_midint, f_int

-- ORDER BY NULL
select f_int from plan_test order by null

-- ORDER BY RAND()
select f_midint from plan_test order by RAND()

-- Order by, '*' expression
select * from plan_test where f_tinyint = 5 order by f_midint

-- Order by, qualified '*' expression
select plan_test.* from plan_test where f_tinyint = 5 order by f_midint

-- Order by, qualified '*' expression
select * from plan_test where f_tinyint = 5 order by plan_test.f_midint

-- Order by with math functions
select * from plan_test where f_tinyint = 5 order by -f_midint

-- Order by with math operations
select * from plan_test where f_tinyint = 5 order by f_tinyint + f_varchar collate utf8_general_ci desc

-- LIMIT
select f_bigint from plan_test where f_tinyint = 9 limit 1

-- limit for scatter
select f_bigint from plan_test limit 1

-- cross-shard expression in parenthesis with limit
select f_int from plan_test where (f_int = 12345 AND f_varchar ='abc') order by f_int limit 5
```

### <p id='7'>流式聚合查询</p>

```sql
--- stream_aggr_cases.json
-- order by count distinct
select f_smallint, count(distinct f_int), f_tinyint k from plan_test group by f_smallint order by k,f_tinyint
```

### <p id='8'>流式排序查询</p>

```sql
--- stream_memory_sort_cases.json
-- scatter aggregate order by references ungrouped column
select f_int, f_smallint, count(*) k from plan_test group by f_int order by k, f_smallint


-- scatter aggregate with memory sort and limit
select f_int, f_smallint, count(*) k from plan_test group by f_int order by k desc,f_int limit 10
```

### <p id='9'>算术运算</p>

```sql
--- calculate_dual_case.json
-- select from nothing
select 1


-- select from dual
select 1 as result from dual

-- add arithmetic
select 1+2 as result from dual

-- minus arithmetic
select 1-2 as result from dual

-- multiply arithmetic
select 3*2 as result from dual

-- divide arithmetic
select 5/2 as result from dual
```

### <p id='10'>Deep Pagination</p>

```sql
--- deepPagination.json
-- Sharding Key Condition in Parenthesis
select * from plan_test where f_varchar ='abc' AND (f_tinyint = 4) limit 5


-- Column Aliasing with Table.Column
select user0_.f_int as col0_ from plan_test user0_ where f_tinyint = 1 order by user0_.f_int desc limit 2
```

### <p id='11'>JOIN 子句(分片、不分片)</p>

```sql
--- join/sharded/join_cases.json
-- Multi-table unique vindex constraint
select user_extra.id from user join user_extra on user.name = user_extra.user_id where user.name = 105

-- Multi-table unique vindex constraint on right table
select user_extra.id from user join user_extra on user.name = user_extra.user_id where user_extra.user_id = 105

-- Multi-table unique vindex constraint on left table of left join
select user_extra.id from user left join user_extra on user.name = user_extra.user_id where user.name = 105

-- Multi-table unique vindex constraint on left-joined right table
select user_extra.id from user left join user_extra on user.name = user_extra.user_id where user_extra.user_id = 105

-- Multi-route unique vindex constraint
select user_extra.id from user join user_extra on user.costly = user_extra.extra_id where user.name = 105

-- Multi-route unique vindex route on both routes
select user_extra.id from user join user_extra on user.costly = user_extra.extra_id where user.name = 105 and user_extra.user_id = 105

-- Multi-route with cross-route constraint
select user_extra.id from user join user_extra on user.costly = user_extra.extra_id where user_extra.user_id = user.costly

-- Multi-route with non-route constraint, should use first route.
select user_extra.id from user join user_extra on user.costly = user_extra.extra_id where 1 = 1

-- Case preservation test
select user_extra.Id from user join user_extra on user.nAME = user_extra.User_Id where user.Name = 105

-- Multi-table, multi-chunk
select music.col from user join music

-- ',' join
select music.col from user, music

-- mergeable sharded join on unique vindex
select user.costly from user join user_extra on user.name = user_extra.user_id

-- mergeable sharded join on unique vindex (parenthesized ON clause)
select user.costly from user join user_extra on (user.name = user_extra.user_id)

-- mergeable sharded join on unique vindex, with a stray condition
select user.costly from user join user_extra on user.costly between 100 and 200 and user.name = user_extra.user_id

-- mergeable sharded join on unique vindex, swapped operands
select user.costly from user join user_extra on user_extra.user_id = user.name

-- mergeable sharded join on unique vindex, and condition
select user.costly from user join user_extra on user.name = 105 and user.name = user_extra.user_id

-- sharded join on unique vindex, inequality
select user.costly from user join user_extra on user.name < user_extra.user_id

-- sharded join, non-col reference RHS
select user.costly from user join user_extra on user.name = 105

-- sharded join, non-col reference LHS
select user.costly from user join user_extra on 105 = user.name

-- sharded join, non-vindex col
select user.costly from user join user_extra on user.costly = user_extra.extra_id

-- col refs should be case-insensitive
select user.costly from user join user_extra on user.NAME = user_extra.User_Id

-- order by on a cross-shard query. Note: this happens only when an order by column is from the second table
select user.predef1 as a, user.predef2 b, music.col c from user, music where user.name = music.user_id and user.name = 101 order by c

-- Order by for join, with mixed cross-shard ordering
select user.predef1 as a, user.predef2, music.col from user join music on user.name = music.id where user.name = 101 order by 1 asc, 3 desc, 2 asc

-- non-ambiguous symbol reference
select user.predef1, user_extra.extra_id from user join user_extra having user_extra.extra_id = 102

-- HAVING multi-route
select user.predef1 as a, user.predef2, user_extra.extra_id from user join user_extra having 1 = 1 and a = 101 and a = user.predef2 and user_extra.extra_id = 101

-- ORDER BY NULL for join
select user.predef1 as a, user.predef2, music.col from user join music on user.name = music.id where user.name = 101 order by null

-- ORDER BY non-key column for join
select user.predef1 as a, user.predef2, music.col from user join music on user.name = music.id where user.name = 101 order by a

-- ORDER BY non-key column for implicit join
select user.predef1 as a, user.predef2, music.col from user, music where user.name = music.id and user.name = 101 order by a

-- ORDER BY RAND() for join
select user.predef1 as a, user.predef2, music.col from user join music on user.name = music.id where user.name = 101 order by RAND()

-- select * from join of authoritative tables
select * from authoritative a join authoritative b on a.user_id=b.user_id

-- select * from intermixing of authoritative table with non-authoritative results in no expansion
select * from authoritative join user on authoritative.user_id = user.name

-- select authoritative.* with intermixing still expands
select user.name, a.*, user.costly from authoritative a join user on a.user_id = user.name

-- auto-resolve anonymous columns for simple route
select costly from user join user_extra on user.name = user_extra.user_id

-- Auto-resolve should work if unique vindex columns are referenced
select name, user_id from user join user_extra

-- RHS route referenced
select user_extra.id from user join user_extra

-- Both routes referenced
select user.costly, user_extra.id from user join user_extra

-- Expression with single-route reference
select user.costly, user_extra.id + user_extra.extra_id from user join user_extra

-- Jumbled references
select user.textcol1, user_extra.id, user.textcol2 from user join user_extra

-- for update
select user.costly from user join user_extra for update

-- Case preservation
select user.Costly, user_extra.Id from user join user_extra


--- join/unsharded/join_cases.json
-- Multi-table unsharded
select t1.f_tinyint from engine_test as t1 join plan_test as t2

-- Multi-table unsharded
select t1.f_tinyint from engine_test as t1, plan_test as t2

-- Multi-table unsharded
select t1.f_tinyint from engine_test as t1 left join plan_test as t2 on t1.f_tinyint = t2.f_tinyint

-- Multi-table unsharded
select t1.f_tinyint from engine_test as t1 straight_join plan_test as t2 on t1.f_tinyint = t2.f_tinyint

-- Multi-table unsharded
select t1.f_tinyint from engine_test as t1 right join plan_test as t2 on t1.f_tinyint = t2.f_tinyint

-- Multi-table unsharded
select t1.f_tinyint from engine_test as t1 inner join plan_test as t2 on t1.f_tinyint = t2.f_tinyint
```

### <p id='12'>子查询 (分片、不分片)</p>

```sql
--- subquery/sharded/subquery_cases.json
-- scatter aggregate in a subquery
select cnt from (select count(*) as cnt from user) t


-- subquery of information_schema with itself
select PLUGIN_NAME pluginName from information_schema.PLUGINS where PLUGIN_NAME in (select PLUGIN_NAME from information_schema.PLUGINS)

-- subquery
select u.id from user_extra join user u where u.name in (select name from user where user.name = u.name and user_extra.extra_id = user.predef1) and u.name in (user_extra.extra_id, 1)

-- ensure subquery reordering gets us a better plan
select u.id from user_extra join user u where u.name in (select costly from user where user.name = 105) and u.name = 105

-- nested subquery
select u.id from user_extra join user u where u.name in (select predef2 from user where user.name = u.name and user_extra.extra_id = user.predef1 and user.name in (select extra_id from user_extra where user_extra.user_id = user.name)) and u.name in (user_extra.extra_id, 1)

-- Correlated subquery in where clause
select id from user where user.predef1 in (select user_extra.extra_id from user_extra where user_extra.user_id = user.name)

-- outer and inner subquery route by same int val
select id from user where name = 105 and user.predef1 in (select user_extra.extra_id from user_extra where user_extra.user_id = 105)

-- outer and inner subquery route by same str val
select id from user where name = '103' and user.predef1 in (select user_extra.extra_id from user_extra where user_extra.user_id = '103')

-- outer and inner subquery route by same outermost column value
select id from user uu where name in (select name from user where name = uu.name and user.predef1 in (select user_extra.extra_id from user_extra where user_extra.user_id = uu.name))

-- cross-shard subquery in IN clause.
select name from user where name in (select costly from user)

-- cross-shard subquery in EXISTS clause.
select id from user where exists (select predef1 from user)

-- cross-shard subquery as expression
select user_id from music where user_id = (select user_id from music)

-- multi-level pullout
select user_id from music where user_id = (select user_id from music where user_id in (select user_id from music))

-- database() call in where clause.
select id from user where database()

-- Select with equals null
select id from music where id = null

-- information_schema query using database() func
SELECT INDEX_LENGTH FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE()

-- index hints, make sure they are not stripped.
select user.costly from user use index(user_costly_uindex)

-- subquery
select id from (select id, textcol1 from user where name = 5) as t

-- subquery with join
select t.name from ( select name from user where name = 105 ) as t join user_extra on t.name = user_extra.user_id

-- subquery with join, and aliased references
select t.name from ( select user.name from user where user.name = 105 ) as t join user_extra on t.name = user_extra.user_id

-- subquery in RHS of join
select t.name from user_extra join ( select name from user where name = 105 ) as t on t.name = user_extra.user_id

-- merge subqueries with single-shard routes
select u.predef1, e.extra_id from ( select predef1 from user where name = 105 ) as u join ( select extra_id from user_extra where user_id = 105 ) as e

-- wire-up on within cross-shard subquery
select t.id from ( select user.id, user.predef2 from user join user_extra on user_extra.extra_id = user.costly ) as t

-- subquery with join primitive (FROM)
select id, t.id from ( select user.id from user join user_extra ) as t

-- order by on a cross-shard subquery
select name from ( select user.name, user.costly from user join user_extra ) as t order by name

-- HAVING uses subquery
select name from user having name in ( select costly from user )

-- Order by subquery column
select u.name from user u join ( select user_id from user_extra where user_id = 105 ) eu on u.name = eu.user_id where u.name = 105 order by eu.user_id


--- subquery/unsharded/subquery_cases.json
-- scatter aggregate in a subquery
select cnt from (select count(*) as cnt from user_unsharded) t

-- subquery
select u.id from user_unsharded_extra join user_unsharded u where u.name in (select name from user_unsharded where user_unsharded.name = u.name and user_unsharded_extra.extra_id = user_unsharded.predef1) and u.name in (user_unsharded_extra.extra_id, 1)

-- ensure subquery reordering gets us a better plan
select u.id from user_unsharded_extra join user_unsharded u where u.name in (select costly from user_unsharded where user_unsharded.name = 105) and u.name = 105

-- nested subquery
select u.id from user_unsharded_extra join user_unsharded u where u.name in (select predef2 from user_unsharded where user_unsharded.name = u.name and user_unsharded_extra.extra_id = user_unsharded.predef1 and user_unsharded.name in (select extra_id from user_unsharded_extra where user_unsharded_extra.user_id = user_unsharded.name)) and u.name in (user_unsharded_extra.extra_id, 1)

-- Correlated subquery in where clause
select id from user_unsharded where user_unsharded.predef1 in (select user_unsharded_extra.extra_id from user_unsharded_extra where user_unsharded_extra.user_id = user_unsharded.name)

-- outer and inner subquery route by same int val
select id from user_unsharded where name = 105 and user_unsharded.predef1 in (select user_unsharded_extra.extra_id from user_unsharded_extra where user_unsharded_extra.user_id = 105)

-- outer and inner subquery route by same str val
select id from user_unsharded where name = '103' and user_unsharded.predef1 in (select user_unsharded_extra.extra_id from user_unsharded_extra where user_unsharded_extra.user_id = '103')

-- outer and inner subquery route by same outermost column value
select id from user_unsharded uu where name in (select name from user_unsharded where name = uu.name and user_unsharded.predef1 in (select user_unsharded_extra.extra_id from user_unsharded_extra where user_unsharded_extra.user_id = uu.name))

-- cross-shard subquery in IN clause.
select name from user_unsharded where name in (select costly from user_unsharded)

-- cross-shard subquery in NOT IN clause.
select name from user_unsharded where name not in (select textcol1 from user_unsharded)

-- cross-shard subquery in EXISTS clause.
select id from user_unsharded where exists (select predef1 from user_unsharded)

-- database() call in where clause.
select id from user_unsharded where database()

-- information_schema query using database() func
SELECT  INDEX_LENGTH, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE()

-- index hints, make sure they are not stripped.
select user_unsharded.costly from user_unsharded use index(user_costly_uindex)

-- subquery
select id from (select id, textcol1 from user_unsharded where name = 5) as t

-- subquery with join, and aliased references
select t.name from ( select user_unsharded.name from user_unsharded where user_unsharded.name = 105 ) as t join user_unsharded_extra on t.name = user_unsharded_extra.user_id

-- subquery in RHS of join
select t.name from user_unsharded_extra join ( select name from user_unsharded where name = 105 ) as t on t.name = user_unsharded_extra.user_id

-- subquery in FROM with cross-shard join
select t.name from ( select name from user_unsharded where name = 105 ) as t join user_unsharded_extra on t.name = user_unsharded_extra.user_id

-- merge subqueries with single-shard routes
select u.predef1, e.extra_id from ( select predef1 from user_unsharded where name = 105 ) as u join ( select extra_id from user_unsharded_extra where user_id = 105 ) as e

-- wire-up on within cross-shard subquery
select t.id from ( select user_unsharded.id, user_unsharded.predef2 from user_unsharded join user_unsharded_extra on user_unsharded_extra.extra_id = user_unsharded.costly ) as t

-- subquery with join primitive (FROM)
select id, t.id from ( select user_unsharded.id from user_unsharded join user_unsharded_extra ) as t

-- order by on a cross-shard subquery
select name from ( select user_unsharded.name, user_unsharded.costly from user_unsharded join user_unsharded_extra ) as t order by name

-- HAVING uses subquery
select name from user_unsharded having name in ( select costly from user_unsharded )

-- ORDER BY after pull-out subquery
select textcol1 from user_unsharded where textcol1 in ( select textcol1 from user_unsharded ) order by textcol1

-- ORDER BY NULL after pull-out subquery
select textcol2 from user_unsharded where textcol2 in ( select textcol2 from user_unsharded ) order by null

-- ORDER BY RAND() after pull-out subquery
select costly from user_unsharded where costly in ( select costly from user_unsharded ) order by rand()

-- Order by subquery column
select u.name from user_unsharded u join ( select user_id from user_unsharded_extra where user_id = 105 ) eu on u.name = eu.user_id where u.name = 105 order by eu.user_id

-- scatter limit after pullout subquery
select textcol2 from user_unsharded where textcol2 in ( select textcol2 from user_unsharded ) order by textcol2 limit 1
```


### <p id='13'>UNION (分片、不分片)</p>

```sql
--- union/sharded/union_cases.json
-- union all between two scatter selects
select id from user_extra union all select id from user_metadata


-- union all between two SelectEqualUnique
select id from user_extra where user_id = 22 union all select id from user_metadata where user_id = 55

-- almost dereks query - two queries with order by and limit being scattered to two different sets of tablets
(select id, email from user_extra order by id asc limit 1) union all (select id, email from user_metadata order by id desc limit 1)

-- Union all
select id, email from user_extra union all select id, email from user_metadata

-- union operations in subqueries (FROM)
select * from (select id, email from user_extra union all select id, email from user_metadata) as t

-- union all between two scatter selects, with order by
(select id from user_extra order by id limit 5) union all (select id from user_extra order by id desc limit 5)

-- union all on scatter and single route
select id from user_extra where user_id = 22 union select id from user_extra where user_id = 22 union all select id from user_extra


--- union/unsharded/union_cases.json
-- union of information_schema
select PLUGIN_NAME from information_schema.PLUGINS union select PLUGIN_NAME from information_schema.PLUGINS

-- 
select predef1, predef2 from ( select predef1, predef2 from user_unsharded where id = 1 union select predef1, predef2 from user_unsharded where id = 3 ) a

-- 
select id, name from user_unsharded where id in ( select id from user_unsharded where id = 1 union select id from user_unsharded where id = 3 )

-- 
( select id from user_unsharded ) union ( select id from unsharded_auto ) order by id limit 5

-- 
select id from user_unsharded union select id from unsharded_auto union select id from unsharded_auto where id in (101)
```

### <p id='14'>DML DELETE (分片、不分片)</p>

```sql
--- dml_delete_sharded.json
-- delete from, no owned vindexes
delete from music where user_id = 1


-- delete from with no where clause
delete from user_extra

-- delete from with no where clause & explicit keyspace reference
delete from :ks.user_extra

-- delete with non-comparison expr
delete from user_extra where user_id between 1 and 2

-- delete from with no index match
delete from user_extra where email = 'jose'

-- delete from with primary id in through IN clause
delete from user_extra where user_id in (1, 2)


--- dml_delete_unsharded.json
-- explicit keyspace reference
delete from :ks.unsharded

-- delete unsharded
delete from unsharded

-- multi-table delete with comma join
delete a from unsharded_authoritative a, unsharded_auto b where a.col1 = b.id and b.val = 'aa'

-- multi-table delete with comma join & explicit keyspace reference
delete a from :ks.unsharded_authoritative a, unsharded_auto b where a.col1 = b.id and b.val = 'aa'

-- multi-table delete with ansi join
delete a from unsharded_authoritative a join unsharded_auto b on a.col1 = b.id where b.val = 'aa'

-- delete with join from subquery
delete foo from unsharded_authoritative as foo left join (select col1 from unsharded_authoritative where col2 is not null order by col2 desc limit 10) as keepers on foo.col1 = keepers.col1 where keepers.col1 is null and foo.col2 is not null and foo.col2 < 1000

-- unsharded delete where inner query references outer query
delete from unsharded_authoritative where col1 = (select id from unsharded_auto where id = unsharded_authoritative.col1)

-- delete from with no where clause
delete from user_unsharded_extra

-- delete with non-comparison expr
delete from user_unsharded_extra where user_id between 1 and 2

-- delete from with no index match
delete from user_unsharded_extra where extra_id = 105

-- delete from with primary id in through IN clause
delete from user_unsharded_extra where user_id in (1, 2)
```


### <p id='15'>DML INSERT (分片、不分片)</p>

```sql
--- dml_insert_sharded.json
-- insert no column list for sharded authoritative table
insert into authoritative(user_id, col1, col2) values(1, 2, 3)


-- insert no column list for sharded authoritative table
insert into authoritative(user_id, col1, col2) values(22,33,44),(333,444,555),(4444,5555,6666)

-- insert with one vindex
insert into user(id, name) values (null, 'aaa')

-- insert with one vindex
insert into user(id, name) values (100, 'bbb')

-- insert with one vindex
insert into user(id, name) values (null, 'ccc'),(null, 'ddd'),(null, 'eee')

-- insert with one vindex
insert into user(id, name) values (null, 'a'),(999, 'b'),(null, 'c')

-- insert with one vindex
insert into user(name) values ('aaaa')

-- insert with one vindex
insert into user(name) values ('aaaa'),('bbbb'),('cccc')

-- insert ignore sharded
insert ignore into user(id) values (1)

-- insert on duplicate key
insert into user(id) values(1) on duplicate key update predef1 = 11

-- insert on duplicate key
insert into user(id) values(1) on duplicate key update predef1 = 111, predef2 = 222

-- insert with default seq
insert into user(id, name) values (default, 'ZS')

-- insert with one vindex and bind var
insert into user(id,name) values (null, 'name')

-- insert with non vindex bool value
insert into user(predef1, textcol1) values (true, false)

-- replace sharded
replace into user(costly, textcol1, textcol2) values (123, 'textcol1', 'textcol2')

-- insert with one vindex and bind var
insert into user(id,name,textcol1,textcol2) values (null,'name','textcol1','textcol2'),(null,'name','textcol1','textcol2'),(null,'name','textcol1','textcol2'),(null,'name','textcol1','textcol2')

-- insert sharded pinned
insert into pinned values (1, '1')

-- insert sharded pinned
insert into pinned(id, name) values (2, '2')

-- insert sharded pinned
insert into pinned(id) values (3)

-- insert sharded pinned
insert into pinned(name) values ('4')

-- insert sharded pinned
insert into pinned(id, name) values (NULL, '5')

-- insert sharded pinned
insert into pinned(id, name) values (NULL, '6'),(NULL, '7'),(NULL, '8')

-- insert sharded pinned
insert ignore into pinned(id) values (1) on duplicate key update name = '11'

-- insert sharded pinned
replace into pinned(id, name) values (2, '22')


--- dml_insert_unsharded.json
-- simple insert unsharded
insert into unsharded values(1, 2)

-- simple upsert unsharded
insert into unsharded values(1, 2) on duplicate key update predef2 = 22

-- unsharded insert, no col list with auto-inc and authoritative column list
insert into unsharded_authoritative values(11,22)

-- insert unsharded, column present
insert into unsharded_auto(id, val) values(1, 'aa')

-- insert unsharded, column absent
insert into unsharded_auto(val) values('aa')

-- insert unsharded, column absent
insert into unsharded_auto(val) values(false)

-- insert unsharded, multi-val
insert into unsharded_auto(id, val) values(4, 'aa'), (null, 'bb')

-- simple replace unsharded
replace into unsharded values(1, 222)

-- simple replace unsharded
replace into unsharded values(3, 4)

-- simple replace unsharded
replace into unsharded values(1, 2)

-- replace unsharded, column present
replace into unsharded_auto(id, val) values(1, 'aa')

-- replace unsharded, column absent
replace into unsharded_auto(val) values('aa')

-- replace unsharded, multi-val
replace into unsharded_auto(id, val) values(5, 'aa'), (6, 'bb')
```

### <p id='16'>DML UPDATE (分片、不分片)</p>

```sql
--- dml_update_sharded.json
-- explicit keyspace reference
update :ks.engine_test set f_tinyint = 1 where f_key = '11'


-- update by primary keyspace id
update engine_test set f_tinyint = 1 where f_key = '11'

-- update by primary keyspace id with alias
update engine_test as engine_test_alias set f_tinyint = 1 where engine_test_alias.f_key = '11'

-- update by primary keyspace id with parenthesized expression
update engine_test set f_tinyint = 1 where (f_key = '11')

-- update by primary keyspace id with multi-part where clause with parens
update engine_test set f_tinyint = 1 where (f_tinyint = 0 and f_key = '11')

-- update by primary keyspace id, changing one vindex column, using order by and limit
update engine_test set f_tinyint = 1 where f_key = '11' order by f_key asc limit 1

-- update by primary keyspace id, stray where clause
update engine_test set f_tinyint = 1 where f_key = f_varchar and f_key = '11'

-- update columns of multi column vindex
update engine_test set f_tinyint = 1, f_varchar = '22' where f_key = '11'

-- update with no primary vindex on where clause (scatter update)
update engine_test set f_tinyint = 1

-- update with non-comparison expr
update engine_test set f_tinyint = 1 where f_key between '11' and '22'

-- update with primary id through IN clause
update engine_test set f_tinyint = 1 where f_key in ('11', '22')

-- update with non-unique key
update engine_test set f_tinyint = 1 where f_varchar = '33'

-- update with where clause with parens
update engine_test set f_tinyint = 1 where (f_varchar = '11' or f_key = '11')

-- update vindex value to null with complex where clause
update engine_test set f_tinyint = 1 where f_key + 11 = '22'


--- dml_update_unsharded.json
-- explicit keyspace reference
update :ks.unsharded set predef2 = 1

-- update unsharded
update unsharded set predef2 = 1

-- update by primary keyspace id
update unsharded set predef2 = 1 where predef1 = 1

-- update by primary keyspace id with alias
update unsharded as unsharded_alias set predef2 = 1 where unsharded_alias.predef1 = 1

-- update by primary keyspace id with parenthesized expression
update unsharded set predef2 = 1 where (predef1 = 1)

-- subqueries in unsharded update
update unsharded set predef2 = (select col1 from unsharded_authoritative limit 1)

-- unsharded union in subquery of unsharded update
update unsharded set predef2 = (select col1 from unsharded_authoritative union select col1 from unsharded_authoritative)

-- unsharded join in subquery of unsharded update
update unsharded set predef2 = (select a.col1 from unsharded_authoritative a join unsharded_authoritative b on a.col1 = b.col1)

-- update with join subquery
update unsharded_authoritative as foo left join (select col1 from unsharded_authoritative where col2 is not null order by col2 desc limit 10) as keepers on foo.col1 = keepers.col1 set foo.col1 = 100 where keepers.col1 is null and foo.col2 is not null and foo.col2 < 1000

-- update by primary keyspace id with multi-part where clause with parens
update unsharded set predef2 = 1 where (predef2 = 0 and predef1 = 1)

-- update by primary keyspace id, changing same vindex twice
update unsharded set predef2 = 0, predef2 = 1 where predef1 = 1

-- update by primary keyspace id, changing one vindex column, using order by and limit
update unsharded set predef2 = 1 where predef2 = 0 order by predef1 asc limit 1

-- update by primary keyspace id, stray where clause
update unsharded set predef2 = 3 where predef1 = predef2 and predef1 = 1

-- update multi-table ansi join
update unsharded_authoritative a join unsharded_auto b on a.col1 = b.id set b.val = 'foo' where b.val = 'aa'

-- update multi-table comma join
update unsharded_authoritative a, unsharded_auto b set b.val = 'foo' where a.col1 = b.id and b.val = 'aa'

-- update with non-comparison expr
update unsharded set predef2 = 1 where predef1 between 1 and 2

-- update with primary id through IN clause
update unsharded set predef2 = 1 where predef1 in (1, 2)

-- update with where clause with parens
update unsharded set predef2 = 1 where (predef2 = 2 or predef1 = 1)

-- unsharded update where inner query references outer query
update unsharded_authoritative set col1 = (select id from unsharded_auto where id = unsharded_authoritative.col1) where col1 = (select predef1 from unsharded)

-- update vindex value to null with complex where clause
update unsharded set predef2 = 1 where predef1 + 1 = 2
```
