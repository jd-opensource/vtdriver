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
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.util.threadpool.impl.VtQueryExecutorService;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import lombok.Getter;

public class ConcatenateGen4Engine implements PrimitiveEngine {
    @Getter
    private final List<PrimitiveEngine> sourceList;

    private Map<Integer, Object> noNeedToTypeCheck;

    public ConcatenateGen4Engine(List<PrimitiveEngine> sourceList, Map<Integer, Object> noNeedToTypeCheck) {
        this.sourceList = sourceList;
        this.noNeedToTypeCheck = noNeedToTypeCheck;
    }

    // NewConcatenate creates a Concatenate primitive. The ignoreCols slice contains the offsets that
    // don't need to have the same type between sources -
    // weight_string() sometimes returns VARBINARY and sometimes VARCHAR
    public ConcatenateGen4Engine(List<PrimitiveEngine> sourceList, List<Integer> ignoreCols) {
        Map<Integer, Object> ignoreTypes = new HashMap<>();
        if (ignoreCols != null) {
            for (Integer i : ignoreCols) {
                ignoreTypes.put(i, null);
            }
        }
        this.sourceList = sourceList;
        this.noNeedToTypeCheck = ignoreTypes;
    }

    @Override
    public String getKeyspaceName() {
        Set<String> ksSet = new LinkedHashSet<>(16, 1);
        for (PrimitiveEngine source : this.sourceList) {
            ksSet.add(source.getKeyspaceName());
        }
        List<String> ksList = new ArrayList<>(ksSet);
        Collections.sort(ksList);
        return String.join("_", ksList);
    }

    @Override
    public String getTableName() {
        List<String> tabList = new ArrayList<>();
        for (PrimitiveEngine source : this.sourceList) {
            tabList.add(source.getTableName());
        }
        return String.join("_", tabList);
    }

    @Override
    public IExecute.ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindValue, boolean wantFields) throws SQLException {
        List<VtResultSet> res;
        try {
            res = execSources(ctx, vcursor, bindValue, wantFields);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Query.Field[] fields = getFields(res);
        long rowsAffected = 0;
        List<List<VtResultValue>> rows = new ArrayList<>();
        for (VtResultSet r : res) {
            rowsAffected += r.getRowsAffected();
            if (!rows.isEmpty() && !r.getRows().isEmpty()
                && rows.get(0).size() != r.getRows().get(0).size()) {
                throw new SQLException("The used SELECT statements have a different number of columns");
            }
            rows.addAll(r.getRows());
        }
        VtResultSet resultSet = new VtResultSet();
        resultSet.setFields(fields);
        resultSet.setRows(rows);
        resultSet.setRowsAffected(rowsAffected);
        return new IExecute.ExecuteMultiShardResponse(resultSet);
    }

    public List<VtResultSet> execSources(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVars, boolean wantfields) throws SQLException, InterruptedException {
        int size = sourceList.size();
        VtResultSet[] results = new VtResultSet[size];
        CountDownLatch latch = new CountDownLatch(size);
        ConcurrentLinkedQueue<SQLException> exceptions = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < size; i++) {
            final int currIndex = i;
            final PrimitiveEngine currSource = sourceList.get(i);
            final Map<String, BindVariable> vars = bindVars == null ? null : new HashMap<>(bindVars);
            VtQueryExecutorService.execute(() -> {
                try {
                    IExecute.ExecuteMultiShardResponse result = currSource.execute(ctx, vcursor, vars, wantfields);
                    results[currIndex] = (VtResultSet) result.getVtRowList();
                } catch (SQLException t) {
                    exceptions.add(t);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        if (!exceptions.isEmpty()) {
            throw exceptions.peek();
        }
        return Arrays.asList(results);
    }

    private void compareFields(Query.Field[] firstFields, Query.Field[] secondFields) throws SQLException {
        if (firstFields.length != secondFields.length) {
            throw new SQLException("The used SELECT statements have a different number of columns");
        }
        for (int i = 0; i < secondFields.length; i++) {
            Query.Field firstField = firstFields[i];
            Query.Field secondField = secondFields[i];
            if (noNeedToTypeCheck != null && noNeedToTypeCheck.containsKey(i)) {
                continue;
            }
            if (firstField.getType() != secondField.getType()) {
                throw new SQLException(
                    String.format("merging field of different types is not supported, name: (%s, %s) types: (%s, %s)", firstField.getName(), secondField.getName(), firstField.getType(),
                        secondField.getType()));
            }
        }
    }

    @Override
    public VtResultSet getFields(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        VtResultSet firstQr = this.sourceList.get(0).getFields(vcursor, bindVariableMap);
        for (int i = 0; i < this.sourceList.size(); i++) {
            PrimitiveEngine source = this.sourceList.get(i);
            if (i == 0) {
                continue;
            }
            VtResultSet qr = source.getFields(vcursor, bindVariableMap);
            this.compareFields(firstQr.getFields(), qr.getFields());
        }
        return firstQr;
    }

    private Query.Field[] getFields(List<VtResultSet> res) throws SQLException {
        if (res == null || res.isEmpty()) {
            return null;
        }
        Query.Field[] fields = null;
        for (VtResultSet r : res) {
            if (r.getFields() == null) {
                continue;
            }
            if (fields == null) {
                fields = r.getFields();
                continue;
            }

            compareFields(fields, r.getFields());
        }
        return fields;
    }

    @Override
    public Boolean canResolveShardQuery() {
        return Boolean.FALSE;
    }

    @Override
    public Boolean needsTransaction() {
        for (PrimitiveEngine source : this.sourceList) {
            if (source.needsTransaction()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<PrimitiveEngine> inputs() {
        return this.sourceList;
    }

}
