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

package com.jd.jdbc.queryservice;

import com.jd.jdbc.pool.InnerConnection;
import com.jd.jdbc.queryservice.util.VtResultSetUtils;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtType;
import com.jd.jdbc.util.KeyspaceUtil;
import io.vitess.proto.Query;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class StreamIterator implements VtIterator<VtResultSet> {
    private final InnerConnection connection;

    private final ResultSet resultSet;

    private Query.Field[] fields = null;

    private VtResultSet currentVtResultSet;

    public StreamIterator(InnerConnection connection, ResultSet resultSet) {
        this.connection = connection;
        this.resultSet = resultSet;
    }

    @Override
    public boolean hasNext() throws SQLException {
        boolean hasNext = resultSet.next();
        if (hasNext) {
            currentVtResultSet = new VtResultSet();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int cols = metaData.getColumnCount();
            if (fields == null) {
                fields = new Query.Field[cols];
                for (int idx = 0, col = 1; idx < cols; idx++, col++) {
                    Query.Field.Builder fieldBuilder = Query.Field.newBuilder();
                    Query.Type queryType = VtType.getQueryType(metaData.getColumnTypeName(col));
                    fieldBuilder.setDatabase(KeyspaceUtil.getLogicSchema(metaData.getCatalogName(col)))
                        .setJdbcClassName(metaData.getColumnClassName(col))
                        .setPrecision(metaData.getPrecision(col))
                        .setIsSigned(metaData.isSigned(col))
                        .setColumnLength(metaData.getColumnDisplaySize(col))
                        .setDecimals(metaData.getScale(col))
                        .setTable(metaData.getTableName(col))
                        .setName(metaData.getColumnLabel(col))
                        .setOrgName(metaData.getColumnName(col))
                        .setType(queryType);
                    fields[idx] = fieldBuilder.build();
                }
            }
            currentVtResultSet.setFields(fields);
            List<List<VtResultValue>> rows = new ArrayList<>();

            List<VtResultValue> vtValueList = new ArrayList<>(cols);
            for (int col = 1; col <= cols; col++) {
                VtResultValue vtResultValue = VtResultSetUtils.getValue(resultSet, col, fields[col - 1].getJdbcClassName(), (int) fields[col - 1].getPrecision(), fields[col - 1].getType());
                vtValueList.add(vtResultValue);
            }
            rows.add(vtValueList);
            currentVtResultSet.setRows(rows);
            currentVtResultSet.setRowsAffected(rows.size());
            currentVtResultSet.setInsertID(-1);
        }
        return hasNext;
    }

    @Override
    public VtResultSet next() throws SQLException {
        return currentVtResultSet;
    }

    @Override
    public void close() throws SQLException {
        if (resultSet != null) {
            resultSet.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
