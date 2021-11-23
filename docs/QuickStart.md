## Quick Start

### 推荐环境：

以下为搭建Vitess多分片场景的最低配置要求

| CPU    | MEMORY | DISK CAPACITY | Operation System   |
| ------ | ------ | ------------- | ------------------ |
| 2 Core | 4 GB   | 20GB          | Ubuntu 16 / CentOS |

* 如果已有Vitess环境，可参考[VtDriver适配](./VtDriver适配.md)进行环境配置后跳过第一步，根据第二步进行数据源配置

### 第一步：搭建Vitess环境

1. Vitess环境搭建

   https://vitess.io/docs/contributing/

   * MySQL建议5.7

   * 如果部署的Vitess环境和项目不在同一机器上，在执行101_initial_cluster.sh前，要在vitess/examples/local/env.sh中修改ETCD_SERVER的ip修改为环境所部署机器的ip (
   不是127.0.0.1或者localhost)

2. 运行测试用例（可选）

   在 vitess/example/local 下执行

    ```shell
    ./101_initial_cluster.sh

    ./201_customer_tablets.sh 

    ./202_move_tables.sh

    ./203_switch_reads.sh

    ./204_switch_writes.sh

    ./205_clean_commerce.sh

    ./301_customer_sharded.sh

    ./302_new_shards.sh 

    ./303_reshard.sh

    ./304_switch_reads.sh

    ./305_switch_writes.sh

    ./306_down_shard_0.sh

    ./307_delete_shard_0.sh
    ```

   在 vitess/example/local/ 创建 vtdriver 文件夹，并将项目目录 vtdriver/src/test/resources/config/ 下的所有文件导入 vtdriver 文件夹

   在 vitess/example/local/vtdriver/ 中依次执行init_mysql_user.sh、create_table.sh

    ```shell
    ./init_mysql_user.sh
    ./create_table.sh
    ```
   
   * init_mysql_user.sh 为上述脚本中创建的库设置默认用户，设置 sql_mode
     
     默认用户名：vtdriver
     
     默认密码：vtdriver_password
     
   * create_table.sh 创建了测试用例中需要的表和对应的vSchema

### 第二步：通过VtDriver链接数据库

1. Java应用pom中引入VtDriver包

   ```xml
   <!-- Version: VtDriver对应的版本号，例如: 1.0.0 -->
   <dependency>
     <groupId>io.vitess.driver</groupId>
     <artifactId>vtdriver</artifactId>
     <version>${Version}</version>
   </dependency>
   ```

2. 配置数据库驱动

   配置驱动(driverClassName)为com.jd.jdbc.vitess.VitessDriver

3. 配置数据源，例如：

   ```yaml
   # ETCD_IP: 第一步中配置的ETCD_SERVER，为ETCD所在机器的IP
   # ETCD_PORT: 访问ETCD的端口
   # keyspace: 逻辑库名，例如上述运行测试用例中创建的commerce和customer
   driver.jdbc.url=jdbc:vitess://{ETCD_IP}:{ETCD_PORT}/keyspace?&serverTimezone=Asia/Shanghai
   driver.jdbc.username=vtdriver
   driver.jdbc.password=vtdriver_password
   ```
