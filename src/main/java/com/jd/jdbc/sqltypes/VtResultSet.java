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

import com.jd.jdbc.vitess.VitessStatement;
import io.vitess.proto.Query;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class VtResultSet implements VtRowList {
    private Query.Field[] fields;

    private long rowsAffected;

    private BigInteger insertID;

    private List<List<VtResultValue>> rows;

    private boolean isDML = false;

    private int curRow = -1;

    private boolean closed = false;

    private VitessStatement owningStatement = null;

    public VtResultSet() {
        this.rows = new ArrayList<>();
        this.insertID = BigInteger.valueOf(0);
    }

    public VtResultSet(BigInteger insertID, long rowsAffected) {
        this.insertID = insertID;
        this.rowsAffected = rowsAffected;
    }

    public VtResultSet(Query.Field[] fields, List<List<VtResultValue>> rows) {
        this.fields = fields;
        this.rows = rows;
        this.insertID = BigInteger.valueOf(0);
    }

    public VtResultSet(long rowsAffected, List<List<VtResultValue>> rows) {
        this.rowsAffected = rowsAffected;
        this.rows = rows;
        this.insertID = BigInteger.valueOf(0);
    }

    @Override
    public VtRowList setDML() {
        this.isDML = true;
        return this;
    }

    public void appendResult(VtResultSet src) throws SQLException {
        appendResult(src, false);
    }

    public void appendResultIgnoreTable(VtResultSet src) throws SQLException {
        appendResult(src, true);
    }

    /**
     * @param src
     */
    private void appendResult(VtResultSet src, boolean ignoreTable) throws SQLException {
        if (src == null) {
            return;
        }
        if (src.rowsAffected == 0 && (src.fields == null || src.fields.length == 0)) {
            return;
        }
        if (this.fields == null) {
            this.fields = src.fields;
        } else {
            Query.Field[] fields = this.fields;
            Query.Field[] srcFields = src.fields;
            if (srcFields != null) {
                if (fields.length != srcFields.length) {
                    throw new SQLException("the table structure of different shards are inconsistent,the field length is not equal the number of columns returned");
                }
                for (int i = 0; i < fields.length; i++) {
                    boolean flag = ignoreTable ? equalsIgnoreTable(fields[i], srcFields[i]) : fields[i].equals(srcFields[i]);
                    if (!flag) {
                        throw new SQLException("the table structure of different shards are inconsistent,the fields are different");
                    }
                }
            }
        }
        this.rowsAffected += src.rowsAffected;
        if (!src.insertID.equals(BigInteger.valueOf(0))) {
            this.insertID = src.insertID;
        }
        if (this.rows == null || this.rows.isEmpty()) {
            rows = src.rows;
        } else if (src.rows != null && !src.rows.isEmpty()) {
            this.rows.addAll(src.rows);
        }
    }

    private boolean equalsIgnoreTable(Query.Field field1, Query.Field field2) {
        if (field1 == field2) {
            return true;
        }
        boolean result = true;
        result = result && field1.getName().equals(field2.getName());
        result = result && field1.getType() == field2.getType();
        result = result && field1.getOrgTable().equals(field2.getOrgTable());
        result = result && field1.getDatabase().equals(field2.getDatabase());
        result = result && field1.getOrgName().equals(field2.getOrgName());
        result = result && (field1.getColumnLength() == field2.getColumnLength());
        result = result && (field1.getCharset() == field2.getCharset());
        result = result && (field1.getDecimals() == field2.getDecimals());
        result = result && (field1.getFlags() == field2.getFlags());
        result = result && (field1.getJdbcType() == field2.getJdbcType());
        result = result && field1.getJdbcClassName().equals(field2.getJdbcClassName());
        return result;
    }

    /**
     * Truncate returns a new Result with all the rows truncated
     * to the specified number of columns.
     *
     * @param l
     * @return
     */
    public VtResultSet truncate(int l) {
        if (l == 0) {
            return this;
        }

        VtResultSet out = new VtResultSet(this.insertID, this.rowsAffected);
        if (this.fields != null) {
            out.fields = Arrays.copyOf(this.fields, l);
        }
        if (this.rows != null) {
            out.rows = new ArrayList<>(this.rows.size());
            for (List<VtResultValue> row : this.rows) {
                out.rows.add(row.subList(0, l));
            }
        }
        return out;
    }

    @Override
    public VtRowList reserve(int maxRows) {
        if (maxRows == 0 || maxRows >= this.rows.size()) {
            return this;
        }
        this.rows = this.rows.subList(0, maxRows);
        return this;
    }

    @Override
    public boolean hasNext() throws SQLException {
        if (closed) {
            throw new SQLException("result set has been closed");
        }
        int total = (rows == null) ? 0 : rows.size();
        if (total == 0) {
            return false;
        }
        return curRow < total - 1;
    }

    @Override
    public List<VtResultValue> next() throws SQLException {
        if (closed) {
            throw new SQLException("result set has been closed");
        }
        ++curRow;
        return rows.get(curRow);
    }

    @Override
    public void close() throws SQLException {
        if (isClosed()) {
            return;
        }

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
    public boolean isQuery() {
        return !isDML;
    }

    @Override
    public int getResultSetSize() {
        if (rows != null) {
            return rows.size();
        }
        return 0;
    }

    @Override
    public VtRowList clone() {
        VtRowList vtRowList = new VtResultSet(this.fields, this.rows);
        return vtRowList;
    }

    public BigInteger getInsertID() {
        return this.insertID;
    }

    public void setInsertID(long setId) {
        this.insertID = BigInteger.valueOf(setId);
    }

    public void setInsertID(BigInteger setId) {
        this.insertID = setId;
    }

}
