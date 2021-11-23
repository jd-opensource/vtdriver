### VtDriver适配已有的Vitess环境

1. 修改ETCD_SERVER
   
   如果部署的Vitess环境和项目不在同一机器上，要在vitess/examples/local/env.sh中修改ETCD_SERVER的ip修改为环境所部署机器的ip (
   不是127.0.0.1或者localhost)，并重启生效。确保项目所在机器可以访问到ETCD

2. 运行测试用例（可选）

   根据项目中的文件
   
   vtdriver/src/test/resources/config/commerce_unsharded.sql
   
   vtdriver/src/test/resources/config/customer_sharded.sql

   分别对单分片和两分片的库创建表；
   
   参考vSchema文件

   vtdriver/src/test/resources/config/vschema_commerce_unsharded.json

   vtdriver/src/test/resources/config/vschema_customer_sharded.json

   更新vSchema；

   设置相关库的sql_mode

   ```sql
   set global sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION';
   ```

3. 链接数据库

   可参考[QuickStart.md](./QuickStart.md)的第二步进行配置