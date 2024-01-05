/*
Copyright 2023 JD Project Authors. Licensed under Apache-2.0.

Copyright 2022 The Vitess Authors.

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

package com.jd.jdbc.planbuilder.semantics;

import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.gen4.AbstractAggregateGen4;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.expr.SQLAggregateExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLCharExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntegerExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNumberExpr;
import io.vitess.proto.Query;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;

/**
 * typer is responsible for setting the type for expressions
 * it does it's work after visiting the children (up), since the children types is often needed to type a node.
 */
public class Typer {
    private static final Type TYPE_INT_32 = new Type(Query.Type.INT32);

    private static final Type DECIMAL = new Type(Query.Type.DECIMAL);

    private static final Type FLOAT_VAL = new Type(Query.Type.FLOAT64);

    @Getter
    private Map<SQLObject, Type> exprTypes;

    public Typer() {
        this.exprTypes = new HashMap<>(16);
    }

    public void up(SQLObject cursor) {
        if (cursor instanceof SQLIntegerExpr) {
            this.exprTypes.put(cursor, TYPE_INT_32);
        }
        if (cursor instanceof SQLCharExpr) {
            // TODO - add system default collation name
            this.exprTypes.put(cursor, new Type(Query.Type.VARCHAR));
        }
        if (cursor instanceof SQLNumberExpr) {
            SQLNumberExpr numberExpr = (SQLNumberExpr) cursor;
            if (numberExpr.getNumber() instanceof BigDecimal) {
                this.exprTypes.put(cursor, DECIMAL);
            }
        }
//        case sqlparser.FloatVal:
//        t.exprTypes[node] = floatval
        if (cursor instanceof SQLAggregateExpr) {
            Engine.AggregateOpcodeG4 aggregateOpcode = AbstractAggregateGen4.SUPPORTED_AGGREGATES.get(((SQLAggregateExpr) cursor).getMethodName().toLowerCase());
            if (aggregateOpcode != null) {
                Query.Type type = AbstractAggregateGen4.OPCODE_TYPE.get(aggregateOpcode);
                if (type != null) {
                    this.exprTypes.put(cursor, new Type(type));
                }
            }
        }
    }

    public void setTypeFor(SQLObject node, Type typ) {
        exprTypes.put(node, typ);
    }
}
