/*
Copyright 2021 JD Project Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.jd.jdbc.spring;

import com.alibaba.druid.pool.DruidDataSource;
import com.baomidou.dynamic.datasource.DynamicRoutingDataSource;
import com.baomidou.dynamic.datasource.ds.ItemDataSource;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.vitess.VitessJdbcUrlParser;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import javax.sql.DataSource;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class VitessDataSourceInitializingBean implements ApplicationContextAware, InitializingBean {

    private static final Log LOG = LogFactory.getLog(VitessDataSourceInitializingBean.class);

    private ApplicationContext applicationContext;

    @Override
    public void afterPropertiesSet() {
        Map<String, DataSource> dataSources = applicationContext.getBeansOfType(DataSource.class);
        initVitessDataSource(dataSources);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    private void initVitessDataSource(Map<String, DataSource> dataSources) {
        for (Map.Entry<String, DataSource> entry : dataSources.entrySet()) {
            DataSource dataSource = entry.getValue();
            warmUpConnectionPool(entry.getKey(), dataSource);
            warmUpDynamicRoutingDataSource(dataSource);
            warmAopProxyDataSource(entry.getKey(), dataSource);
        }
    }

    private void warmUpConnectionPool(String dataSourceName, DataSource dataSource) {
        if (isVitessDataSource(dataSource)) {
            LOG.info("VtDriver datasource start init: " + dataSourceName);
            try (Connection connection = dataSource.getConnection()) {
            } catch (SQLException e) {
                LOG.info("VtDriver datasource init error: " + e.getMessage());
            }
        }
    }

    private void warmUpDynamicRoutingDataSource(DataSource dataSource) {
        String className = dataSource.getClass().getName();
        if (Objects.equals("com.baomidou.dynamic.datasource.DynamicRoutingDataSource", className)) {
            com.baomidou.dynamic.datasource.DynamicRoutingDataSource dynamicRoutingDataSource = (DynamicRoutingDataSource) dataSource;
            Map<String, DataSource> currentDataSources = dynamicRoutingDataSource.getCurrentDataSources();
            for (Map.Entry<String, DataSource> dataSourceEntry : currentDataSources.entrySet()) {
                DataSource entryDataSource = dataSourceEntry.getValue();
                String entryDataSourceClassName = entryDataSource.getClass().getName();
                if (Objects.equals("com.baomidou.dynamic.datasource.ds.ItemDataSource", entryDataSourceClassName)) {
                    com.baomidou.dynamic.datasource.ds.ItemDataSource itemDataSource = (ItemDataSource) entryDataSource;
                    warmUpConnectionPool(itemDataSource.getName(), itemDataSource.getRealDataSource());
                } else {
                    warmUpConnectionPool(dataSourceEntry.getKey(), entryDataSource);
                }
            }
        }
    }

    private boolean isVitessDataSource(DataSource dataSource) {
        String className = dataSource.getClass().getName();
        switch (className) {
            case "com.alibaba.druid.pool.DruidDataSource":
            case "com.alibaba.druid.spring.boot.autoconfigure.DruidDataSourceWrapper":
                DruidDataSource druidDataSource = (DruidDataSource) dataSource;
                return VitessJdbcUrlParser.acceptsUrl(druidDataSource.getUrl());
            case "com.zaxxer.hikari.HikariDataSource":
                HikariDataSource hikariDataSource = (HikariDataSource) dataSource;
                return VitessJdbcUrlParser.acceptsUrl(hikariDataSource.getJdbcUrl());
            case "org.apache.commons.dbcp.BasicDataSource":
                org.apache.commons.dbcp.BasicDataSource basicDataSource = (org.apache.commons.dbcp.BasicDataSource) dataSource;
                return VitessJdbcUrlParser.acceptsUrl(basicDataSource.getUrl());
            case "org.apache.commons.dbcp2.BasicDataSource":
                org.apache.commons.dbcp2.BasicDataSource basicDataSource2 = (org.apache.commons.dbcp2.BasicDataSource) dataSource;
                return VitessJdbcUrlParser.acceptsUrl(basicDataSource2.getUrl());
            case "com.mchange.v2.c3p0.ComboPooledDataSource":
                ComboPooledDataSource comboPooledDataSource = (ComboPooledDataSource) dataSource;
                return VitessJdbcUrlParser.acceptsUrl(comboPooledDataSource.getJdbcUrl());
            case "org.apache.tomcat.dbcp.dbcp2.BasicDataSource":
                org.apache.tomcat.dbcp.dbcp2.BasicDataSource dbcp2BasicDataSource = (org.apache.tomcat.dbcp.dbcp2.BasicDataSource) dataSource;
                return VitessJdbcUrlParser.acceptsUrl(dbcp2BasicDataSource.getUrl());
            case "org.apache.tomcat.jdbc.pool.DataSource":
                org.apache.tomcat.jdbc.pool.DataSource tomcatDataSource = (org.apache.tomcat.jdbc.pool.DataSource) dataSource;
                return VitessJdbcUrlParser.acceptsUrl(tomcatDataSource.getUrl());
            default:
                return false;
        }
    }

    private void warmAopProxyDataSource(String dataSourceName, DataSource dataSource) {
        if (!AopUtils.isAopProxy(dataSource)) {
            return;
        }
        Class<?> targetClass = AopUtils.getTargetClass(dataSource);
        String className = targetClass.getName();
        Class clazz;
        switch (className) {
            case "com.alibaba.druid.spring.boot.autoconfigure.DruidDataSourceWrapper":
            case "com.alibaba.druid.pool.DruidDataSource":
                clazz = com.alibaba.druid.pool.DruidDataSource.class;
                break;
            case "com.zaxxer.hikari.HikariDataSource":
                clazz = com.zaxxer.hikari.HikariDataSource.class;
                break;
            case "org.apache.commons.dbcp.BasicDataSource":
                clazz = org.apache.commons.dbcp.BasicDataSource.class;
                break;
            case "org.apache.commons.dbcp2.BasicDataSource":
                clazz = org.apache.commons.dbcp2.BasicDataSource.class;
                break;
            case "com.mchange.v2.c3p0.ComboPooledDataSource":
                clazz = com.mchange.v2.c3p0.ComboPooledDataSource.class;
                break;
            case "org.apache.tomcat.dbcp.dbcp2.BasicDataSource":
                clazz = org.apache.tomcat.dbcp.dbcp2.BasicDataSource.class;
                break;
            case "org.apache.tomcat.jdbc.pool.DataSource":
                clazz = org.apache.tomcat.jdbc.pool.DataSource.class;
                break;
            default:
                return;
        }
        try {
            DataSource targetDataSource = (DataSource) dataSource.unwrap(clazz);
            warmUpConnectionPool(dataSourceName, targetDataSource);
        } catch (Exception e) {
            LOG.info("init AopProxy datasource error: " + e.getMessage());
        }
    }
}