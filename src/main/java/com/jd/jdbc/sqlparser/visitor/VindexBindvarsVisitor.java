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

package com.jd.jdbc.sqlparser.visitor;

import com.google.protobuf.ByteString;
import io.vitess.proto.Query;
import com.jd.jdbc.sqlparser.ast.expr.*;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqltypes.VtValue;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class VindexBindvarsVisitor extends SQLASTVisitorAdapter {

    private final static Log log = LogFactory.getLog(VindexBindvarsVisitor.class);

    Map<String, Query.BindVariable> bindvars;
    String vindex;
    Set<VtValue> valuess;
    String sql;

    public VindexBindvarsVisitor(Map<String, Query.BindVariable> bindvars, String vindex) {
        this.bindvars = bindvars;
        this.vindex = vindex.toLowerCase();
        this.valuess = new HashSet<>();
    }

    @Override
    public boolean visit(SQLSelectStatement x) {
        sql = x.getSelect().toString();
        log.info("sql is " + sql);
        return true;
    }

    @Override
    public boolean visit(SQLBinaryOpExpr x) {
        if (x.getLeft() instanceof SQLIdentifierExpr) {
            String name = ((SQLIdentifierExpr) x.getLeft()).getName().toLowerCase();
            if (!name.equals(this.vindex)) {
                return super.visit(x);
            }

            if (x.getRight() instanceof SQLCharExpr) {
                this.bindvars.put(name, Query.BindVariable.newBuilder().setTypeValue(Query.Type.CHAR_VALUE).
                        setValue(ByteString.copyFromUtf8(((SQLCharExpr) x.getRight()).getText())).build());
                return super.visit(x);
            }

            if (x.getRight() instanceof SQLIntegerExpr) {
                this.bindvars.put(name, Query.BindVariable.newBuilder().setTypeValue(Query.Type.INT64_VALUE).
                        setValue(ByteString.copyFromUtf8(((SQLIntegerExpr) x.getRight()).getNumber().toString())).build());
                return super.visit(x);
            }

            if (x.getRight() instanceof SQLNumberExpr) {
                Number number = ((SQLNumberExpr) x.getRight()).getNumber();
                if (number instanceof Integer) {
                    this.bindvars.put(name, Query.BindVariable.newBuilder().setTypeValue(Query.Type.INT64_VALUE).
                            setValue(ByteString.copyFromUtf8(number.toString())).build());
                    return super.visit(x);
                }
                if (number instanceof Float) {
                    this.bindvars.put(name, Query.BindVariable.newBuilder().setTypeValue(Query.Type.FLOAT64_VALUE).
                            setValue(ByteString.copyFromUtf8(number.toString())).build());
                    return super.visit(x);
                }
                if (number instanceof Double) {
                    this.bindvars.put(name, Query.BindVariable.newBuilder().setTypeValue(Query.Type.FLOAT64_VALUE).
                            setValue(ByteString.copyFromUtf8(number.toString())).build());
                    return super.visit(x);
                }
                if (number instanceof BigDecimal) {
                    this.bindvars.put(name, Query.BindVariable.newBuilder().setTypeValue(Query.Type.FLOAT64_VALUE).
                            setValue(ByteString.copyFromUtf8(number.toString())).build());
                    return super.visit(x);
                }
                if (number instanceof Byte) {
                    this.bindvars.put(name, Query.BindVariable.newBuilder().setTypeValue(Query.Type.BIT_VALUE).
                            setValue(ByteString.copyFromUtf8(number.toString())).build());
                    return super.visit(x);
                }
                if (number instanceof Short) {
                    this.bindvars.put(name, Query.BindVariable.newBuilder().setTypeValue(Query.Type.INT64_VALUE).
                            setValue(ByteString.copyFromUtf8(number.toString())).build());
                    return super.visit(x);
                }
                if (number instanceof Long) {
                    this.bindvars.put(name, Query.BindVariable.newBuilder().setTypeValue(Query.Type.INT64_VALUE).
                            setValue(ByteString.copyFromUtf8(number.toString())).build());
                    return super.visit(x);
                }
            }
        }
        return super.visit(x);
    }

    public VtValue[] getValuess() {
        VtValue[] values = new VtValue[valuess.size()];
        return valuess.toArray(values);
    }

    public String getSql() {
        return sql;
    }
}
