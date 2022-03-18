package com.jd.vtdriver.spring.boot.autoconfigure;

import com.alibaba.druid.pool.DruidDataSource;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class VtDriverDataSourceInit implements ApplicationContextAware, InitializingBean {

    private final static Log LOG = LogFactory.getLog(VtDriverDataSourceInit.class);

    private static ApplicationContext applicationContext;

    @Override
    public void afterPropertiesSet() {
        Map<String, DataSource> dataSources = VtDriverDataSourceInit.getBeans(DataSource.class);
        initVtDriverDataSource(dataSources);
    }

    @Override
    public synchronized void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        VtDriverDataSourceInit.applicationContext = applicationContext;
    }

    public static void initVtDriverDataSource(Map<String, DataSource> dataSources) {
        for (Map.Entry<String, DataSource> entry : dataSources.entrySet()) {
            DataSource dataSource = entry.getValue();
            if (isVtDriverDataSource(dataSource)) {
                try {
                    LOG.info("VtDriver datasource init: " + entry.getKey());
                    Connection conn = dataSource.getConnection();
                    conn.close();
                } catch (SQLException e) {
                    LOG.info("VtDriver datasource init error: " + e.getMessage());
                }
            }
        }
    }

    public static <T> Map<String, T> getBeans(Class<T> clazz) {
        return applicationContext.getBeansOfType(clazz);
    }

    public static boolean isVtDriverDataSource(DataSource dataSource) {
        String prefix = "jdbc:vitess";
        String className = dataSource.getClass().getName();
        switch (className) {
            case "com.alibaba.druid.pool.DruidDataSource":
                DruidDataSource druidDataSource = (DruidDataSource) dataSource;
                return druidDataSource.getUrl().startsWith(prefix);
            case "com.zaxxer.hikari.HikariDataSource":
                HikariDataSource hikariDataSource = (HikariDataSource) dataSource;
                return hikariDataSource.getJdbcUrl().startsWith(prefix);
            case "org.apache.commons.dbcp2.BasicDataSource":
                BasicDataSource basicDataSource = (BasicDataSource) dataSource;
                return basicDataSource.getUrl().startsWith(prefix);
            case "com.mchange.v2.c3p0.ComboPooledDataSource":
                ComboPooledDataSource comboPooledDataSource = (ComboPooledDataSource) dataSource;
                return comboPooledDataSource.getJdbcUrl().startsWith(prefix);
            case "org.apache.tomcat.dbcp.dbcp2.BasicDataSource":
                org.apache.tomcat.dbcp.dbcp2.BasicDataSource dbcp2BasicDataSource = (org.apache.tomcat.dbcp.dbcp2.BasicDataSource) dataSource;
                return dbcp2BasicDataSource.getUrl().startsWith(prefix);
            case "org.apache.tomcat.jdbc.pool.DataSource":
                org.apache.tomcat.jdbc.pool.DataSource tomcatDataSource = (org.apache.tomcat.jdbc.pool.DataSource) dataSource;
                return tomcatDataSource.getUrl().startsWith(prefix);
            default:
                return false;
        }
    }
}
