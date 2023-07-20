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

package com.jd.jdbc.vitess;

import com.jd.jdbc.sqltypes.VtRowList;
import com.jd.jdbc.sqltypes.VtType;
import io.vitess.proto.Query;
import java.sql.SQLException;

public class VitessResultSetMetaData extends AbstractVitessResultSetMetaData {

    private final VtRowList vtResult;

    public VitessResultSetMetaData(VtRowList result) {
        vtResult = result;
    }

    private Query.Field getField(int column) throws SQLException {
        if (vtResult == null) {
            throw new SQLException("empty result set metadata");
        }
        return vtResult.getFields()[column - 1];
    }

    @Override
    public int getColumnCount() throws SQLException {
        if (vtResult == null) {
            throw new SQLException("empty result set metadata");
        }
        return vtResult.getFields().length;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return columnNullableUnknown;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return getField(column).getIsSigned();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return getField(column).getColumnLength();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getField(column).getName();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        String name = getField(column).getOrgName();
        return name.isEmpty() ? getField(column).getName() : name;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return getField(column).getDatabase();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return (int) getField(column).getPrecision();
    }

    @Override
    public int getScale(int column) throws SQLException {
        return getField(column).getDecimals();
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return getField(column).getTable();
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return getField(column).getDatabase();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return VtType.DataTypeConverter.fromTypeString(getField(column).getType().name()).getSqlType(getPrecision(column));
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return VtType.DataTypeConverter.fromTypeString(getField(column).getType().name()).getMySqlTypeName(getPrecision(column), isSigned(column));
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        if (vtResult == null) {
            throw new SQLException("empty result set metadata");
        }
        return getField(column).getJdbcClassName();
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return getField(column).getIsCaseSensitive();
    }
}
