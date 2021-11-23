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

package com.jd.jdbc.queryservice.util;

import com.jd.jdbc.sqltypes.VtResultValue;
import io.vitess.proto.Query;
import java.sql.ResultSet;
import java.sql.SQLException;

public class VtResultSetUtils {
    public static VtResultValue getValue(final ResultSet resultSet, final int columnIndex, final String type, final int precision, final Query.Type vtType) throws SQLException {
        Object value;
        switch (type) {
            case "java.lang.Boolean":
                // BIT对应Query.Type=BIT
                // tinyint(1) singed 范围-128~127, 对应Query.Type=BIT, 这个时候应该存为int
                if (precision == 1) {
                    int intResult = resultSet.getInt(columnIndex);
                    value = intResult == 0 && resultSet.wasNull() ? null : intResult;
                    break;
                }
                long longBooleanResult = resultSet.getLong(columnIndex);
                value = longBooleanResult == 0 && resultSet.wasNull() ? null : longBooleanResult;
                break;
            case "java.lang.Integer":
                int intResult = resultSet.getInt(columnIndex);
                value = intResult == 0 && resultSet.wasNull() ? null : intResult;
                break;
            case "java.lang.Long":
                long longResult = resultSet.getLong(columnIndex);
                value = longResult == 0 && resultSet.wasNull() ? null : longResult;
                break;
            case "java.lang.Float":
                float floatResult = resultSet.getFloat(columnIndex);
                value = floatResult == 0 && resultSet.wasNull() ? null : floatResult;
                break;
            case "java.lang.Double":
                double doubleResult = resultSet.getDouble(columnIndex);
                value = doubleResult == 0 && resultSet.wasNull() ? null : doubleResult;
                break;
            case "java.math.BigDecimal":
                value = resultSet.getBigDecimal(columnIndex);
                break;
            case "java.sql.Time":
            case "java.sql.Timestamp":
            case "java.sql.Date":
                value = resultSet.getBytes(columnIndex);
                break;
            case "java.lang.String":
                value = resultSet.getString(columnIndex);
                break;
            default:
                value = resultSet.getObject(columnIndex);
        }
        if (value == null) {
            return VtResultValue.NULL;
        }
        return new VtResultValue(value, vtType);
    }
}