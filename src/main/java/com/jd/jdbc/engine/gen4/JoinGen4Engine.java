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

package com.jd.jdbc.engine.gen4;

import com.jd.jdbc.IExecute;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.sqltypes.SqlTypes;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtRowList;
import com.jd.jdbc.sqltypes.VtStreamResultSet;
import com.jd.jdbc.srvtopo.BindVariable;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JoinGen4Engine implements PrimitiveEngine {
    private Engine.JoinOpcode opcode;

    /**
     * Left and Right are the LHS and RHS primitives
     * of the Join. They can be any primitive.
     */
    private PrimitiveEngine left;

    private PrimitiveEngine right;

    /**
     * Cols defines which columns from the left
     * or right results should be used to build the
     * return result. For results coming from the
     * left query, the index values go as -1, -2, etc.
     * For the right query, they're 1, 2, etc.
     * If Cols is {-1, -2, 1, 2}, it means that
     * the returned result will be {Left0, Left1, Right0, Right1}.
     */
    private List<Integer> cols;

    /**
     * Vars defines the list of joinVars that need to
     * be built from the LHS result before invoking
     * the RHS subqquery.
     */
    private Map<String, Integer> vars;

    public JoinGen4Engine(Engine.JoinOpcode opcode, Map<String, Integer> vars) {
        this.opcode = opcode;
        this.vars = vars;
        this.cols = new ArrayList<>();
    }

    @Override
    public String getKeyspaceName() {
        if (this.left.getKeyspaceName().equalsIgnoreCase(this.right.getKeyspaceName())) {
            return this.left.getKeyspaceName();
        }
        return this.left.getKeyspaceName() + "_" + this.right.getKeyspaceName();
    }

    @Override
    public String getTableName() {
        return this.left.getTableName() + "_" + this.right.getTableName();
    }

    @Override
    public IExecute.ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        Map<String, BindVariable> joinVars = new LinkedHashMap<>();

        IExecute.ExecuteMultiShardResponse leftResultResponse = this.left.execute(ctx, vcursor, bindVariableMap, wantFields);

        VtRowList leftRowList = leftResultResponse.getVtRowList();
        if (leftRowList == null) {
            throw new SQLException("VtRowList is null");
        }

        VtResultSet resultSet = new VtResultSet();

        VtResultSet leftResult = (VtResultSet) leftRowList;
        if ((leftResult.getRows() == null || leftResult.getRows().isEmpty()) && wantFields) {
            for (Map.Entry<String, Integer> entry : this.vars.entrySet()) {
                joinVars.put(entry.getKey(), BindVariable.NULL_BIND_VARIABLE);
            }
            VtResultSet rightResult = this.right.getFields(vcursor, combineVars(bindVariableMap, joinVars));
            resultSet.setFields(joinFields(leftResult.getFields(), rightResult.getFields(), this.cols));
            return new IExecute.ExecuteMultiShardResponse(resultSet);
        }

        if (leftResult.getRows() != null) {
            for (List<VtResultValue> leftRow : leftResult.getRows()) {
                for (Map.Entry<String, Integer> entry : this.vars.entrySet()) {
                    joinVars.put(entry.getKey(), SqlTypes.valueBindVariable(leftRow.get(entry.getValue())));
                }

                IExecute.ExecuteMultiShardResponse rightResultResponse = this.right.execute(ctx, vcursor, combineVars(bindVariableMap, joinVars), wantFields);

                VtRowList rightRowList = rightResultResponse.getVtRowList();
                if (rightRowList == null) {
                    throw new SQLException("VtRowList is null");
                }

                VtResultSet rightResult = (VtResultSet) rightRowList;

                if (wantFields) {
                    wantFields = false;
                    resultSet.setFields(joinFields(leftResult.getFields(), rightResult.getFields(), this.cols));
                }

                for (List<VtResultValue> rightRow : rightResult.getRows()) {
                    resultSet.getRows().add(joinRows(leftRow, rightRow, this.cols));
                }

                if (this.opcode == Engine.JoinOpcode.LeftJoin && (rightResult.getRows() == null || rightResult.getRows().isEmpty())) {
                    resultSet.getRows().add(joinRows(leftRow, null, this.cols));
                }
                if (vcursor.exceedsMaxMemoryRows(resultSet.getRows().size())) {
                    throw new SQLException("in-memory row count exceeded allowed limit of " + vcursor.maxMemoryRows());
                }
            }
        }
        return new IExecute.ExecuteMultiShardResponse(resultSet);
    }

    @Override
    public IExecute.VtStream streamExecute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindValue, boolean wantFields) throws SQLException {
        IExecute.VtStream leftStream = this.left.streamExecute(ctx, vcursor, bindValue, wantFields);
        return new IExecute.VtStream() {
            private IExecute.VtStream leftStreamResult = leftStream;

            private IExecute.VtStream rightStreamResult;

            @Override
            public VtRowList fetch(boolean wantFields) throws SQLException {
                return this.internalFetch(wantFields);
            }

            private VtRowList internalFetch(boolean wantFields) throws SQLException {
                Map<String, BindVariable> joinVars = new HashMap<>();
                VtResultSet resultSet = new VtResultSet();

                VtStreamResultSet leftStreamResultSet = new VtStreamResultSet(this.leftStreamResult, wantFields);
                while (leftStreamResultSet.hasNext()) {
                    List<VtResultValue> leftRow = leftStreamResultSet.next();
                    for (Map.Entry<String, Integer> var : vars.entrySet()) {
                        joinVars.put(var.getKey(), SqlTypes.valueBindVariable(leftRow.get(var.getValue())));
                    }
                    boolean rowSent = false;
                    this.rightStreamResult = right.streamExecute(ctx, vcursor, combineVars(bindValue, joinVars), wantFields);
                    VtStreamResultSet rightStreamResultSet = new VtStreamResultSet(rightStreamResult, wantFields);
                    if (wantFields) {
                        // This code is currently unreachable because the first result
                        // will always be just the field info, which will cause the outer
                        // wantfields code path to be executed. But this may change in the future.
                        wantFields = false;
                        resultSet.setFields(joinFields(leftStreamResultSet.getFields(), rightStreamResultSet.getFields(), cols));
                    }
                    while (rightStreamResultSet.hasNext()) {
                        rowSent = true;
                        List<VtResultValue> rightRow = rightStreamResultSet.next();
                        resultSet.getRows().add(joinRows(leftRow, rightRow, cols));
                    }
                    rightStreamResultSet.close();
                    if (opcode == Engine.JoinOpcode.LeftJoin && !rowSent) {
                        resultSet.setRows(new ArrayList<List<VtResultValue>>() {
                            {
                                add(new ArrayList<VtResultValue>() {{
                                    addAll(joinRows(leftRow, null, cols));
                                }});
                            }
                        });
                    }
                }
                if (wantFields) {
                    wantFields = false;
                    for (Map.Entry<String, Integer> var : vars.entrySet()) {
                        joinVars.put(var.getKey(), BindVariable.NULL_BIND_VARIABLE);
                    }
                    VtResultSet rightResultSet = right.getFields(vcursor, null);
                    resultSet.setFields(joinFields(leftStreamResultSet.getFields(), rightResultSet.getFields(), cols));
                }
                return resultSet;
            }

            @Override
            public void close() throws SQLException {
                if (this.rightStreamResult != null) {
                    this.rightStreamResult.close();
                    this.rightStreamResult = null;
                }
                if (leftStream != null) {
                    this.leftStreamResult.close();
                    this.leftStreamResult = null;
                }
            }
        };
    }

    @Override
    public Boolean canResolveShardQuery() {
        return Boolean.FALSE;
    }

    @Override
    public VtResultSet getFields(Vcursor vcursor, Map<String, BindVariable> bindvars) throws SQLException {
        Map<String, BindVariable> joinVars = new HashMap<>();
        VtResultSet resultSet = new VtResultSet();
        VtResultSet leftResult = this.left.getFields(vcursor, bindvars);
        for (Map.Entry<String, Integer> var : this.vars.entrySet()) {
            joinVars.put(var.getKey(), BindVariable.NULL_BIND_VARIABLE);
        }
        VtResultSet rightResult = this.right.getFields(vcursor, combineVars(bindvars, joinVars));
        resultSet.setFields(joinFields(leftResult.getFields(), rightResult.getFields(), this.cols));
        return resultSet;
    }

    @Override
    public Boolean needsTransaction() {
        return this.right.needsTransaction() || this.left.needsTransaction();
    }

    @Override
    public List<PrimitiveEngine> inputs() {
        return new ArrayList<PrimitiveEngine>() {{
            add(left);
            add(right);
        }};
    }

    private Map<String, BindVariable> combineVars(Map<String, BindVariable> bindVariableMap1, Map<String, BindVariable> bindVariableMap2) {
        Map<String, BindVariable> newBindVar = new HashMap<>(16, 1);
        if (bindVariableMap1 == null) {
            bindVariableMap1 = new LinkedHashMap<>();
        }
        if (bindVariableMap2 == null) {
            bindVariableMap2 = new LinkedHashMap<>();
        }
        newBindVar.putAll(bindVariableMap1);
        newBindVar.putAll(bindVariableMap2);
        return newBindVar;
    }

    private Query.Field[] joinFields(Query.Field[] leftFields, Query.Field[] rightFields, List<Integer> cols) {
        Query.Field[] fields = new Query.Field[cols.size()];
        for (int i = 0; i < cols.size(); i++) {
            Integer index = cols.get(i);
            if (index < 0) {
                fields[i] = leftFields[-index - 1];
                continue;
            }
            fields[i] = rightFields[index - 1];
        }
        return fields;
    }

    private List<VtResultValue> joinRows(List<VtResultValue> leftRow, List<VtResultValue> rightRow, List<Integer> cols) {
        List<VtResultValue> row = new ArrayList<>();
        for (Integer index : cols) {
            if (index < 0) {
                row.add(leftRow.get(-index - 1));
                continue;
            }
            // right row can be null on left joins
            if (rightRow != null) {
                row.add(rightRow.get(index - 1));
            } else {
                row.add(new VtResultValue(null, Query.Type.NULL_TYPE));
            }
        }
        return row;
    }
}
