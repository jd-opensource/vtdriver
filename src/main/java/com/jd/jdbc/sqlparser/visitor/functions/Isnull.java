/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
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
package com.jd.jdbc.sqlparser.visitor.functions;

import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLMethodInvokeExpr;
import com.jd.jdbc.sqlparser.visitor.SQLEvalVisitor;

import java.util.List;

import static com.jd.jdbc.sqlparser.visitor.SQLEvalVisitor.*;

public class Isnull implements Function {

    public final static Isnull instance = new Isnull();

    public Object eval(SQLEvalVisitor visitor, SQLMethodInvokeExpr x) {
        final List<SQLExpr> parameters = x.getParameters();
        if (parameters.size() == 0) {
            return EVAL_ERROR;
        }

        SQLExpr condition = parameters.get(0);
        condition.accept(visitor);
        Object itemValue = condition.getAttributes().get(EVAL_VALUE);
        if (itemValue == EVAL_VALUE_NULL) {
            return Boolean.TRUE;
        } else if (itemValue == null) {
            return null;
        } else {
            return Boolean.FALSE;
        }
    }
}
