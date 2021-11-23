/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jd.jdbc.sqlparser.dialect.mysql;

import com.google.protobuf.ByteString;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtSmartNormalizeVisitor;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.sqltypes.VtNumberRange;
import io.vitess.proto.Query;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;


public class SmartNormalizer {
    private static final Log logger = LogFactory.getLog(SmartNormalizer.class);

    public static SmartNormalizerResult normalize(SQLStatement stmt, Map<String, Query.BindVariable> bindVariableMap, String charEncoding) throws SQLException {
        VtSmartNormalizeVisitor smartNormalizeVisitor = new VtSmartNormalizeVisitor(new ArrayList<>(), new StringBuilder(), true);
        smartNormalizeVisitor.init(bindVariableMap);
        stmt.accept(smartNormalizeVisitor);
        return new SmartNormalizerResult(stmt, smartNormalizeVisitor.getQuery(),
            parameterToVtValue(smartNormalizeVisitor.getParameters(), charEncoding));
    }

    private static Map<String, Query.BindVariable> parameterToVtValue(List<Object> parameterList, String charEncoding) throws SQLException {
        Map<String, Query.BindVariable> bindVariableMap = new LinkedHashMap<>();
        for (int i = 0; i < parameterList.size(); i++) {
            Object o = parameterList.get(i);

            Query.BindVariable.Builder builder = Query.BindVariable.newBuilder();

            if (o == null) {
                builder.setType(Query.Type.NULL_TYPE).setValue(ByteString.copyFrom(new byte[] {}));
            } else {
                Class<?> clazz = o.getClass();
                if (clazz == Query.BindVariable.class) {
                    builder = ((Query.BindVariable) o).toBuilder();
                } else if (clazz == String.class) {
                    byte[] bytes = StringUtils.getBytes((String) o, charEncoding);
                    builder.setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom(bytes));
                } else if (clazz == Boolean.class) {
                    builder.setType(Query.Type.BIT).setValue(ByteString.copyFrom((Boolean) o ? new byte[] {1} : new byte[] {0}));
                } else if (clazz == BigDecimal.class) {
                    builder.setType(Query.Type.DECIMAL).setValue(ByteString.copyFrom((((BigDecimal) o).toEngineeringString()).getBytes()));
                } else if (clazz == BigInteger.class) {
                    if (((BigInteger) o).compareTo(new BigInteger(String.valueOf(VtNumberRange.INT64_MIN))) >= 0
                        && ((BigInteger) o).compareTo(new BigInteger(String.valueOf(VtNumberRange.INT64_MAX))) <= 0) {
                        builder.setType(Query.Type.INT64).setValue(ByteString.copyFrom(((BigInteger) o).toString(10).getBytes()));
                    } else {
                        builder.setType(Query.Type.UINT64).setValue(ByteString.copyFrom(((BigInteger) o).toString(10).getBytes()));
                    }
                } else if (clazz == Byte.class) {
                    builder.setType(Query.Type.INT8).setValue(ByteString.copyFrom(String.valueOf(o).getBytes()));
                } else if (clazz == Double.class) {
                    builder.setType(Query.Type.FLOAT64).setValue(ByteString.copyFrom(String.valueOf(o).getBytes()));
                } else if (clazz == Float.class) {
                    builder.setType(Query.Type.FLOAT32).setValue(ByteString.copyFrom(String.valueOf(o).getBytes()));
                } else if (clazz == Integer.class) {
                    builder.setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(o).getBytes()));
                } else if (clazz == Long.class) {
                    builder.setType(Query.Type.INT64).setValue(ByteString.copyFrom(String.valueOf(o).getBytes()));
                } else if (clazz == Short.class) {
                    builder.setType(Query.Type.INT16).setValue(ByteString.copyFrom(String.valueOf(o).getBytes()));
                } else {
                    logger.error("unknown class " + clazz);
                    throw new SQLException("unknown class " + clazz);
                }
            }
            bindVariableMap.put(String.valueOf(i), builder.build());
        }

        return bindVariableMap;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public static class SmartNormalizerResult {
        private final SQLStatement stmt;

        private final String query;

        private Map<String, Query.BindVariable> bindVariableMap;
    }
}
