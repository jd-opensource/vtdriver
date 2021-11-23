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
import java.sql.SQLException;
import lombok.Getter;

public class StatefulConnection implements AutoCloseable {

    private static final Log logger = LogFactory.getLog(StatefulConnection.class);

    private final StatefulConnectionPool pool;

    @Getter
    private final long connID;

    private final boolean tainted;

    private final boolean enforceTimeout;

    @Getter
    private InnerConnection conn;

    public StatefulConnection(StatefulConnectionPool pool, InnerConnection conn, long connID, boolean tainted, boolean enforceTimeout) {
        this.pool = pool;
        this.conn = conn;
        this.connID = connID;
        this.tainted = tainted;
        this.enforceTimeout = enforceTimeout;
    }

    @Override
    public void close() {
        this.pool.unregister(connID);
        if (conn != null) {
            try {
                conn.close();
                this.conn = null;
            } catch (Exception e) {
                logger.error("close exception " + e.getMessage(), e);
            }
        }
    }

    public ExecuteResult execute(String sql) throws SQLException {
        logger.debug("conn: " + connID + ", " + sql);
        return conn.execute(sql);
    }

    public void commit() throws SQLException {
        logger.debug("conn: " + connID + ", commit");
        conn.commit();
    }

    public void rollback() throws SQLException {
        logger.debug("conn: " + connID + ", rollback");
        if (conn.isClosed()) {
            return;
        }
        conn.rollback();
    }

    //Release is used when the connection will not be used ever again.
    //The underlying dbConn is removed so that this connection cannot be used by mistake.
    public void release() {
        if (conn == null) {
            return;
        }

//        this.pool.unregister(connID);
        //sc.dbConn.Recycle()
        close();
    }

    public void setAutoCommitFalse() throws SQLException {
        conn.setAutoCommitFalse();
    }

    // UnlockUpdateTime returns the connection to the pool. The connection remains active.
    // This method is idempotent and can be called multiple times
    public void unlockUpdateTime() {
        unlock(true);
    }

    public void unlock(boolean updateTime) {
        if (conn == null) {
            return;
        }

        try {
            if (conn.isClosed()) {
                release();
            } else {
                pool.markAsNotInUse(connID, updateTime);
            }
        } catch (Exception e) {
            logger.error("unlock exception", e);
        }
    }
}
