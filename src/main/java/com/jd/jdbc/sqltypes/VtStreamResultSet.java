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

package com.jd.jdbc.sqltypes;

import com.jd.jdbc.IExecute;
import com.jd.jdbc.vitess.VitessStatement;
import io.vitess.proto.Query;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.List;

public class VtStreamResultSet implements VtRowList {
    private final VitessStatement owningStatement = null;

    private IExecute.VtStream vtStream;

    private Query.Field[] fields;

    private VtRowList fetched;

    private SQLException savedException;

    private boolean closed = false;

    private int maxRows = 0;

    private int readRows = 0;

    public VtStreamResultSet(IExecute.VtStream vtStream, boolean wantFields) {
        this.vtStream = vtStream;
        if (vtStream == null) {
            return;
        }

        try {
            fetched = vtStream.fetch(wantFields);
        } catch (SQLException e) {
            savedException = e;
            return;
        }
        if (fetched != null) {
            fields = fetched.getFields();
        }
    }

    @Override
    public Query.Field[] getFields() {
        return this.fields;
    }

    @Override
    public long getRowsAffected() {
        return 0;
    }

    @Override
    public BigInteger getInsertID() {
        return BigInteger.valueOf(0);
    }

    @Override
    public boolean isQuery() {
        return true;
    }

    @Override
    public VtRowList setDML() {
        //this must be a query
        return this;
    }

    @Override
    public VtRowList reserve(int maxRows) {
        this.maxRows = maxRows;
        return this;
    }

    @Override
    public boolean hasNext() throws SQLException {
        if (closed) {
            throw new SQLException("result set has been closed");
        }

        if (savedException != null) {
            throw savedException;
        }

        if (maxRows != 0 && readRows >= maxRows) {
            return false;
        }

        boolean hasNext = fetched.hasNext();
        if (!hasNext) {
            fetched = vtStream.fetch(false);
            hasNext = fetched.hasNext();
        }
        return hasNext;
    }

    @Override
    public List<VtResultValue> next() throws SQLException {
        if (closed) {
            throw new SQLException("result set has been closed");
        }

        if (savedException != null) {
            throw savedException;
        }

        ++readRows;
        return fetched.next();
    }

    @Override
    public void close() throws SQLException {
        if (isClosed()) {
            return;
        }

        this.vtStream.close();
        this.vtStream = null;

        if (owningStatement != null) {
            owningStatement.removeOpenResultSet(this);
        }

        this.closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public int getResultSetSize() {
        return 1;
    }
}
