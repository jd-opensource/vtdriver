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

package com.jd.jdbc.engine;

import com.jd.jdbc.IExecute;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtRowList;
import com.jd.jdbc.srvtopo.BindVariable;
import io.vitess.proto.Query;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ProjectionEngine implements PrimitiveEngine {

    List<String> cols;

    List<EvalEngine.Expr> exprs;

    PrimitiveEngine input;

    @Override
    public String getKeyspaceName() {
        return this.input.getKeyspaceName();
    }

    @Override
    public String getTableName() {
        return this.input.getKeyspaceName();
    }

    @Override
    public IExecute.ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        IExecute.ExecuteMultiShardResponse result = this.input.execute(ctx, vcursor, bindVariableMap, wantFields);
        VtResultSet resultSet = (VtResultSet) result.getVtRowList();

        return getExecuteMultiShardResponse(resultSet, bindVariableMap, wantFields, vcursor.getCharEncoding());
    }

    @Override
    public IExecute.ExecuteMultiShardResponse mergeResult(VtResultSet resultSet, Map<String, BindVariable> bindValues, boolean wantFields) throws SQLException {
        IExecute.ExecuteMultiShardResponse executeMultiShardResponse = this.input.mergeResult(resultSet, bindValues, wantFields);
        VtResultSet vtResultSet = (VtResultSet) executeMultiShardResponse.getVtRowList();
        return getExecuteMultiShardResponse(vtResultSet, bindValues, wantFields, null);
    }

    @Override
    public IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindValueList) throws SQLException {
        IExecute.ResolvedShardQuery resolvedShardQuery = this.input.resolveShardQuery(ctx, vcursor, bindValueList);
        return new IExecute.ResolvedShardQuery(resolvedShardQuery.getRss(), resolvedShardQuery.getQueries());
    }

    @Override
    public IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindValueList, Map<String, String> switchTableMap) throws SQLException {
        IExecute.ResolvedShardQuery resolvedShardQuery = this.input.resolveShardQuery(ctx, vcursor, bindValueList, switchTableMap);
        return new IExecute.ResolvedShardQuery(resolvedShardQuery.getRss(), resolvedShardQuery.getQueries());
    }

    @Override
    public Boolean canResolveShardQuery() {
        return this.input.canResolveShardQuery();
    }

    /**
     * @param ctx
     * @param vcursor
     * @param bindValues
     * @param wantFields
     * @return
     * @throws Exception
     */
    @Override
    public IExecute.VtStream streamExecute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindValues, boolean wantFields) throws SQLException {
        IExecute.VtStream vtStream = this.input.streamExecute(ctx, vcursor, bindValues, wantFields);
        return new IExecute.VtStream() {
            private IExecute.VtStream stream = vtStream;

            private boolean fetched = false;

            @Override
            public VtRowList fetch(boolean wantFields) throws SQLException {
                VtResultSet vtResultSet = new VtResultSet();
                if (fetched) {
                    return vtResultSet;
                }

                vtResultSet = (VtResultSet) stream.fetch(wantFields);
                EvalEngine.ExpressionEnv env = new EvalEngine.ExpressionEnv(bindValues);
                if (wantFields) {
                    addFields(vtResultSet, bindValues);
                }
                List<List<VtResultValue>> rows = new ArrayList<>();
                for (List<VtResultValue> row : vtResultSet.getRows()) {
                    env.setRow(row);
                    for (EvalEngine.Expr expr : exprs) {
                        EvalEngine.EvalResult res = expr.evaluate(env);
                        row.add(convertToVtResultValue(res, vcursor.getCharEncoding()));
                    }
                    rows.add(row);
                }
                vtResultSet.setRows(rows);
                fetched = true;
                return vtResultSet;
            }

            @Override
            public void close() throws SQLException {
                if (stream != null) {
                    stream.close();
                    stream = null;
                }
            }
        };
    }

    private void addFields(VtResultSet qr, Map<String, BindVariable> bindVariableMap) throws SQLException {
        EvalEngine.ExpressionEnv env = new EvalEngine.ExpressionEnv(bindVariableMap);
        Query.Field[] existedFields = qr.getFields();
        if (existedFields == null) {
            existedFields = new Query.Field[0];
        }

        Query.Field[] newFields = Arrays.copyOf(existedFields, existedFields.length + this.cols.size());
        for (int i = 0; i < this.cols.size(); i++) {
            EvalEngine.Expr expr = this.exprs.get(i);
            String name = getColumnName(cols.get(i), expr, bindVariableMap);
            Query.Type type = expr.type(env);
            Query.Field.Builder builder = Query.Field.newBuilder().setName(name).setType(type);
            switch (type) {
                case INT32:
                case INT64:
                    builder.setType(Query.Type.INT64)
                        .setJdbcClassName("java.lang.Long")
                        .setColumnLength(19)
                        .setPrecision(19)
                        .setIsSigned(true);
                    break;
                case VARBINARY:
                    int length = expr.evaluate(env).value().toString().length();
                    builder.setJdbcClassName("java.lang.String")
                        .setColumnLength(length)
                        .setPrecision(length);
                    break;
                case FLOAT64:
                    BigDecimal bd = BigDecimal.valueOf(expr.evaluate(env).getFval());
                    bd = bd.setScale(4, RoundingMode.HALF_UP);
                    String planString = bd.toPlainString();
                    int precision = planString.contains(".") ? planString.length() - 1 : planString.length();
                    if (bd.signum() < 0) {
                        precision -= 1;
                    }
                    builder.setType(Query.Type.DECIMAL)
                        .setJdbcClassName("java.math.BigDecimal")
                        .setColumnLength(precision)
                        .setPrecision(precision)
                        .setIsSigned(true)
                        .setDecimals(bd.scale());
                    break;
                case DECIMAL:
                    BigDecimal bd2 = expr.evaluate(env).getBigDecimal();
                    String planString2 = bd2.toPlainString();
                    int precision2 = planString2.contains(".") ? planString2.length() - 1 : planString2.length();
                    if (bd2.signum() < 0) {
                        precision2 -= 1;
                    }
                    if (expr instanceof EvalEngine.BinaryOp) {
                        precision2 += 1;
                    }
                    builder.setJdbcClassName("java.math.BigDecimal")
                        .setColumnLength(precision2)
                        .setPrecision(precision2)
                        .setIsSigned(true)
                        .setDecimals(bd2.scale());
                    break;
            }
            newFields[existedFields.length + i] = builder.build();
        }
        qr.setFields(newFields);
    }

    private String getColumnName(final String alias, final EvalEngine.Expr expr, final Map<String, BindVariable> bindVariableMap) throws SQLException {
        if (StringUtils.isEmpty(alias) || alias.contains("?")) {
            StringBuilder sb = new StringBuilder();
            expr.output(sb, false, bindVariableMap);
            return sb.toString();
        }
        return alias;
    }

    @Override
    public Boolean needsTransaction() {
        return false;
    }

    private IExecute.ExecuteMultiShardResponse getExecuteMultiShardResponse(VtResultSet resultSet, Map<String, BindVariable> bindVariableMap, boolean wantFields, String charEncoding) throws SQLException {
        EvalEngine.ExpressionEnv env = new EvalEngine.ExpressionEnv(bindVariableMap);

        if (wantFields) {
            this.addFields(resultSet, bindVariableMap);
        }

        List<List<VtResultValue>> rows = new ArrayList<>();
        for (List<VtResultValue> row : resultSet.getRows()) {
            env.setRow(row);
            for (EvalEngine.Expr expr : this.exprs) {
                EvalEngine.EvalResult res = expr.evaluate(env);
                row.add(convertToVtResultValue(res, charEncoding));
            }
            rows.add(row);
        }
        resultSet.setRows(rows);
        return new IExecute.ExecuteMultiShardResponse(resultSet);
    }

    private static VtResultValue convertToVtResultValue(EvalEngine.EvalResult res, String charEncoding) throws SQLException {
        VtResultValue resultValue;
        switch (res.getType()) {
            case FLOAT64:
                EvalEngine.EvalResult evalResult1 = new EvalEngine.EvalResult(BigDecimal.valueOf(res.getFval()).setScale(4, RoundingMode.HALF_UP), Query.Type.DECIMAL);
                resultValue = evalResult1.resultValue();
                break;
            case VARBINARY:
                EvalEngine.EvalResult evalResult2 = new EvalEngine.EvalResult(res.getBytes(), Query.Type.VARBINARY);
                Charset cs = StringUtils.isEmpty(charEncoding) ? Charset.defaultCharset() : Charset.forName(charEncoding);
                resultValue = VtResultValue.newVtResultValue(Query.Type.VARBINARY, new String(evalResult2.getBytes(), cs));
                break;
            default:
                resultValue = res.resultValue();
        }
        return resultValue;
    }
}
