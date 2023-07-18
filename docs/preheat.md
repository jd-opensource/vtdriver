# VtDriver预热

应用启动时需要先连接元数据，获取MySQL的连接信息，然后再与底层的mysql建立连接，这个时间确实会比使用MySQL要长，因为分片场景要连接到多个MySQL。

## 预热方式
### Spring XML配置方式
```
<bean id="vitessDataSourceInitializingBean" class="com.jd.jdbc.spring.VitessDataSourceInitializingBean"/>
```
### Spring Java Config配置方式
```
@Bean
public VitessDataSourceInitializingBean vitessDataSourceInitializingBean() {
    return new VitessDataSourceInitializingBean();
}
```
### SpringBoot也可以直接使用vtdriver-spring-boot-starter