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

import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtSmartNormalizeVisitor;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.sqltypes.VtNumberRange;
import com.jd.jdbc.srvtopo.BindVariable;
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

    public static SmartNormalizerResult normalize(SQLStatement stmt, Map<String, BindVariable> bindVariableMap, String charEncoding) throws SQLException {
        VtSmartNormalizeVisitor smartNormalizeVisitor = new VtSmartNormalizeVisitor(new ArrayList<>(), new StringBuilder(), true);
        smartNormalizeVisitor.init(bindVariableMap);
        stmt.accept(smartNormalizeVisitor);
        return new SmartNormalizerResult(stmt, smartNormalizeVisitor.getQuery(),
            parameterToVtValue(smartNormalizeVisitor.getParameters(), charEncoding));
    }

    private static Map<String, BindVariable> parameterToVtValue(List<Object> parameterList, String charEncoding) throws SQLException {
        Map<String, BindVariable> bindVariableMap = new LinkedHashMap<>();
        for (int i = 0; i < parameterList.size(); i++) {
            Object o = parameterList.get(i);
            BindVariable bindVariable;

            if (o == null) {
                bindVariable = BindVariable.NULL_BIND_VARIABLE;
            } else {
                Class<?> clazz = o.getClass();
                if (clazz == BindVariable.class) {
                    bindVariable = (BindVariable) o;
                } else if (clazz == String.class) {
                    byte[] bytes = StringUtils.getBytes((String) o, charEncoding);
                    bindVariable = new BindVariable(bytes, Query.Type.VARBINARY);
                } else if (clazz == Boolean.class) {
                    bindVariable = new BindVariable((Boolean) o ? new byte[] {1} : new byte[] {0}, Query.Type.BIT);
                } else if (clazz == BigDecimal.class) {
                    bindVariable = new BindVariable((((BigDecimal) o).toEngineeringString()).getBytes(), Query.Type.DECIMAL);
                } else if (clazz == BigInteger.class) {
                    if (((BigInteger) o).compareTo(VtNumberRange.BIGINTEGER_INT64_MIN) >= 0
                        && ((BigInteger) o).compareTo(VtNumberRange.BIGINTEGER_INT64_MAX) <= 0) {
                        bindVariable = new BindVariable(((BigInteger) o).toString(10).getBytes(), Query.Type.INT64);
                    } else {
                        bindVariable = new BindVariable(((BigInteger) o).toString(10).getBytes(), Query.Type.UINT64);
                    }
                } else if (clazz == Byte.class) {
                    bindVariable = new BindVariable(String.valueOf(o).getBytes(), Query.Type.INT8);
                } else if (clazz == Double.class) {
                    bindVariable = new BindVariable(String.valueOf(o).getBytes(), Query.Type.FLOAT64);
                } else if (clazz == Float.class) {
                    bindVariable = new BindVariable(String.valueOf(o).getBytes(), Query.Type.FLOAT32);
                } else if (clazz == Integer.class) {
                    bindVariable = new BindVariable(String.valueOf(o).getBytes(), Query.Type.INT32);
                } else if (clazz == Long.class) {
                    bindVariable = new BindVariable(String.valueOf(o).getBytes(), Query.Type.INT64);
                } else if (clazz == Short.class) {
                    bindVariable = new BindVariable(String.valueOf(o).getBytes(), Query.Type.INT16);
                } else {
                    logger.error("unknown class " + clazz);
                    throw new SQLException("unknown class " + clazz);
                }
            }
            bindVariableMap.put(String.valueOf(i), bindVariable);
        }

        return bindVariableMap;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public static class SmartNormalizerResult {
        private final SQLStatement stmt;

        private final String query;

        private Map<String, BindVariable> bindVariableMap;
    }
}
