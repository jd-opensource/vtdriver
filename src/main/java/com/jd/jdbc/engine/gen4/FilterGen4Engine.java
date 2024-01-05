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

package com.jd.jdbc.engine.gen4;

import com.jd.jdbc.IExecute;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.evalengine.EvalResult;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.srvtopo.BindVariable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class FilterGen4Engine implements PrimitiveEngine {

    private EvalEngine.Expr predicate;

    private SQLExpr astPredicate;

    private PrimitiveEngine input;

    public FilterGen4Engine() {
    }

    public FilterGen4Engine(SQLExpr astPredicate, EvalEngine.Expr predicate) {
        this.astPredicate = astPredicate;
        this.predicate = predicate;
    }

    @Override
    public String getKeyspaceName() {
        return this.input.getKeyspaceName();
    }

    @Override
    public String getTableName() {
        return this.input.getTableName();
    }

    @Override
    public IExecute.ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        IExecute.ExecuteMultiShardResponse result = this.getInput().execute(ctx, vcursor, bindVariableMap, wantFields);
        if (this.predicate == null) {
            return result;
        }

        EvalEngine.ExpressionEnv env = new EvalEngine.ExpressionEnv(bindVariableMap);
        List<List<VtResultValue>> newResultRows = new ArrayList<>();

        VtResultSet resultSet = (VtResultSet) result.getVtRowList();
        for (List<VtResultValue> rv : resultSet.getRows()) {
            env.setRow(rv);
            EvalResult evalResult = this.predicate.evaluate(env);
            long intEvalResult = evalResult.value().toInt();
            if (intEvalResult == EvalEngine.TRUE_FLAG) {
                newResultRows.add(rv);
            }
        }
        resultSet.setRows(newResultRows);
        return result;
    }

    @Override
    public List<PrimitiveEngine> inputs() {
        return Collections.singletonList(this.input);
    }

    @Override
    public Boolean needsTransaction() {
        return false;
    }
}
