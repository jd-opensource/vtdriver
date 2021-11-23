/*
Copyright 2021 JD Project Authors. Licensed under Apache-2.0.

Copyright 2019 The Vitess Authors.

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

package com.jd.jdbc.pool;

import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.JdbcConnection;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class InnerConnection implements AutoCloseable {
    private static final Log logger = LogFactory.getLog(InnerConnection.class);

    private final Connection connection;

    private final ConnectionImpl connectionImpl;

    public InnerConnection(Connection conn) throws SQLException {
        this.connection = conn;
        this.connectionImpl = HikariUtil.getConnectionImpl(conn);
    }

    public JdbcConnection getConnectionImpl() {
        return connectionImpl;
    }

    public Connection getConnection() {
        return connection;
    }

    public ExecuteResult execute(String sql) throws SQLException {
        synchronized (this) {
            Statement statement = this.connection.createStatement();
            boolean queryFlag = statement.execute(sql, Statement.RETURN_GENERATED_KEYS);
            return new ExecuteResult(statement, queryFlag);
        }
    }

    public ResultSet streamExecute(String sql) throws SQLException {
        synchronized (this) {
            Statement statement = this.connection.createStatement();
            statement.setFetchSize(Integer.MIN_VALUE);
            return statement.executeQuery(sql);
        }
    }

    public void commit() throws SQLException {
        synchronized (this) {
            connection.commit();
            connection.setAutoCommit(true);
        }
    }

    public void rollback() throws SQLException {
        synchronized (this) {
            connection.rollback();
            connection.setAutoCommit(true);
        }
    }

    @Override
    public void close() throws SQLException {
        synchronized (this) {
            this.connection.close();
        }
    }

    public boolean isClosed() throws SQLException {
        return connection.isClosed();
    }

    public void setAutoCommitFalse() throws SQLException {
        connection.setAutoCommit(false);
    }

}
