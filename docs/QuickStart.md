## Quick Start

### 推荐配置：

以下为搭建Vitess多分片场景的最低配置要求

| CPU    | MEMORY | DISK CAPACITY | Operation System   |
| ------ | ------ | ------------- | ------------------ |
| 2 Core | 4 GB   | 20GB          | Ubuntu 16 / CentOS |

* 如果已有Vitess环境，可参考[VtDriver适配](./VtDriver适配.md)进行环境配置后跳过第一步，根据第二步进行数据源配置 

可以选择以下任意一种方式搭建vtdriver的运行环境:

* [使用 Docker 镜像搭建](#docker)
* [本地环境搭建](#local)

## <p id="docker">使用 Docker 镜像搭建</p>

#### 1. 下载vtdriver的代码:
```shell
git clone git@github.com:vtdriverio/vtdriver.git
```

#### 2. 切换当前目录到`src/test/resources/vitess_env`, 执行`./setup.sh`:
```shell
cd vtdriver/src/test/resource/vitess_env
./setup.sh
```

这一步会拉取vitess源码, 构建一个名为`vtdriver-env`的镜像, 其启动的vitess环境中里面包含了一个cell、两个Keyspace(一个单分片的`commerce`和一个两分片`customer`)。

请确保`docker`已经正确安装并启动。

#### 3. 启动

如果在本机的docker上搭建环境，且只在本机进行连接测试，运行下面的命令以默认的`bridge`网络模式启动容器即可：
```shell
docker run -e HOST_IP=127.0.0.1 -it --name=vtdriver-env -p16100:16100 -p16101:16101 -p16102:16102 -p16300:16300 -p16301:16301 -p16302:16302 -p16400:16400 -p16401:16401 -p16402:16402 -p15000:15000 -p15001:15001 -p15306:15306 -p15100:15100 -p15101:15101 -p15102:15102 -p15300:15300 -p15301:15301 -p15302:15302 -p15400:15400 -p15401:15401 -p15402:15402 -p2379:2379 -p17100:17100 -p17101:17101 -p17102:17102 -p17300:17300 -p17301:17301 -p17302:17302 -p17400:17400 -p17401:17401 -p17402:17402 vitess/vtdriver-env:latest 
```
启动之后，可访问`http://127.0.0.1:15000`查看vitess是否被正常启动，正常情况下，简单配置vtdriver项目后即可运行测试用例。


如果是在服务器上使用docker搭建，即想要在外部服务器内的docker容器进行访问，则需要以`host`网络模式启动容器, 容器共享服务器的网络环境:
```shell
docker run -e HOST_IP=<服务器IP> -itd --name=vtdriver-env --network=host vitess/vtdriver-env:latest
```

下面是一些对外暴露的端口和配置信息:
```
VTGATE
mysql服务端口: 15306, 账号: mysql_user, 密码: mysql_password
web端口: 15001

9个tablet的web端口:
15100, 15101, 15102
15300, 15301, 15302
15400, 15401, 15402
及其grpc服务端口：
16100, 16101, 16102
16300, 16301, 16302
16400, 16401, 16402

MYSQL
账号vtdriver, 密码vtdriver_password, 9台mysql实例端口分别是:
customer: 17100, 17101, 17102
commerce: 17300, 17301, 17302, 17400, 17401, 17402
尾号0的是master。

ETCD
etcd端口: 2379

VTCTLD
web端口: 15000
```

### <p id="local">本地环境搭建</p>

1. Vitess环境搭建

   https://vitess.io/docs/contributing/

   * MySQL建议5.7

   * 如果部署的Vitess环境和vtdriver项目不在同一机器上，在执行101_initial_cluster.sh前，要在vitess/examples/local/env.sh中修改ETCD_SERVER的ip修改为环境所部署机器的ip (
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
   
   * init_mysql_user.sh 为各MySQL服务创建了vtdriver直连所需的默认用户，并设置了 sql_mode
     
     默认用户名：vtdriver
     
     默认密码：vtdriver_password
     
   * create_table.sh 创建了测试用例中需要的表和对应的vSchema

### 通过VtDriver链接数据库

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