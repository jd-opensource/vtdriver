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
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.vitess.VitessJdbcUrlParser;
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
            if (isVitessDataSource(dataSource)) {
                LOG.info("VtDriver datasource start init: " + entry.getKey());
                try (Connection connection = dataSource.getConnection()) {

                } catch (SQLException e) {
                    LOG.info("VtDriver datasource init error: " + e.getMessage());
                }
            }
        }
    }

    private boolean isVitessDataSource(DataSource dataSource) {
        String className = dataSource.getClass().getName();
        switch (className) {
            case "com.alibaba.druid.pool.DruidDataSource":
                DruidDataSource druidDataSource = (DruidDataSource) dataSource;
                return VitessJdbcUrlParser.acceptsUrl(druidDataSource.getUrl());
            case "com.zaxxer.hikari.HikariDataSource":
                HikariDataSource hikariDataSource = (HikariDataSource) dataSource;
                return VitessJdbcUrlParser.acceptsUrl(hikariDataSource.getJdbcUrl());
            case "org.apache.commons.dbcp2.BasicDataSource":
                BasicDataSource basicDataSource = (BasicDataSource) dataSource;
                return VitessJdbcUrlParser.acceptsUrl(basicDataSource.getUrl());
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
}