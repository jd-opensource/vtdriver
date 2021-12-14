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
import com.jd.jdbc.concurrency.AllErrorRecorder;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.planbuilder.MultiQueryPlan;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtRowList;
import com.jd.jdbc.sqltypes.VtStreamResultSet;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.util.threadpool.impl.VtQueryExecutorService;
import com.jd.jdbc.vitess.mysql.VitessPropertyKey;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Getter;

public class ConcatenateEngine implements PrimitiveEngine {
    @Getter
    private final List<PrimitiveEngine> sourceList;

    public ConcatenateEngine(List<PrimitiveEngine> sourceList) {
        this.sourceList = sourceList;
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
        VtResultSet resultSet = new VtResultSet();

        List<IExecute.ResolvedShardQuery> shardQueryList = new ArrayList<>();
        List<Map<String, BindVariable>> batchBindVariableMap = new ArrayList<>();
        for (PrimitiveEngine source : sourceList) {
            IExecute.ResolvedShardQuery shardQueries = source.resolveShardQuery(ctx, vcursor, bindValue);
            shardQueryList.add(shardQueries);
            batchBindVariableMap.add(bindValue);
        }

        PrimitiveEngine primitive = MultiQueryPlan.buildMultiQueryPlan(sourceList, shardQueryList, batchBindVariableMap);

        List<IExecute.ExecuteMultiShardResponse> executeMultiShardResponses;
        List<VtRowList> vtRowLists = new ArrayList<>();

        Integer maxRows = (Integer) ctx.getContextValue(VitessPropertyKey.MAX_ROWS.getKeyName());

        executeMultiShardResponses = primitive.batchExecute(ctx, vcursor, false);
        for (IExecute.ExecuteMultiShardResponse executeResponse : executeMultiShardResponses) {
            VtRowList partResult = executeResponse.getVtRowList().reserve(maxRows);

            vtRowLists.add(partResult);
            if (resultSet.getFields() == null) {
                resultSet.setFields(partResult.getFields());
            }

            this.compareFields(resultSet.getFields(), partResult.getFields());

            VtResultSet partResultSet = (VtResultSet) partResult;
            if (partResultSet.getRows().size() > 0) {
                resultSet.getRows().addAll(partResultSet.getRows());
                if (resultSet.getRows().get(0).size() != partResultSet.getRows().get(0).size()) {
                    throw new SQLException("The used SELECT statements have a different number of columns");
                }
                resultSet.setRowsAffected(resultSet.getRowsAffected() + partResultSet.getRowsAffected());
            }
        }

        return new IExecute.ExecuteMultiShardResponse(resultSet);
    }

    private void compareFields(Query.Field[] firstFields, Query.Field[] secondFields) throws SQLException {
        if (firstFields.length != secondFields.length) {
            throw new SQLException("The used SELECT statements have a different number of columns");
        }
        for (int i = 0; i < secondFields.length; i++) {
            Query.Field firstField = firstFields[i];
            Query.Field secondField = secondFields[i];
            if (firstField.getType() != secondField.getType()) {
                throw new SQLException(String.format("column field type does not match for name: (%s, %s) types: (%s, %s)", firstField.getName(), secondField.getName(), firstField.getType(),
                    secondField.getType()));
            }
        }
    }

    @Override
    public IExecute.VtStream streamExecute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindValue, boolean wantFields) throws SQLException {
        List<IExecute.VtStream> sourceStreamList = new ArrayList<>();
        ReentrantLock lock = new ReentrantLock();

        if (this.sourceList.size() == 1) {
            sourceStreamList.add(this.sourceList.get(0).streamExecute(ctx, vcursor, bindValue, wantFields));
        } else {
            CountDownLatch countDownLatch = new CountDownLatch(this.sourceList.size());
            AllErrorRecorder errorRecorder = new AllErrorRecorder();
            for (PrimitiveEngine engine : this.sourceList) {
                VtQueryExecutorService.execute(() -> {
                    try {
                        IExecute.VtStream sourceStream = engine.streamExecute(ctx, vcursor, bindValue, wantFields);
                        lock.lock();
                        try {
                            sourceStreamList.add(sourceStream);
                        } finally {
                            lock.unlock();
                        }
                    } catch (Exception e) {
                        errorRecorder.recordError(e);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                errorRecorder.recordError(e);
                Thread.currentThread().interrupt();
            }
            errorRecorder.throwException();
        }

        return new IExecute.VtStream() {
            private final List<IExecute.VtStream> sources = sourceStreamList;

            private List<VtStreamResultSet> sourceStreamResultList;

            private Query.Field[] seenFields;

            @Override
            public VtRowList fetch(boolean wantFields) throws SQLException {
                VtResultSet vtResultSet = new VtResultSet();
                if (sources == null) {
                    return vtResultSet;
                }
                if (this.sourceStreamResultList == null) {
                    initSouceStreamResultList();
                }
                for (VtStreamResultSet sourceStream : sourceStreamResultList) {
                    if (sourceStream.hasNext()) {
                        List<VtResultValue> row = sourceStream.next();
                        vtResultSet.getRows().add(row);
                        vtResultSet.setRowsAffected(vtResultSet.getRowsAffected() + 1);
                    }
                }
                if (wantFields && this.seenFields != null) {
                    vtResultSet.setFields(this.seenFields);
                }
                return vtResultSet;
            }

            private void initSouceStreamResultList() throws SQLException {
                if (sources == null) {
                    return;
                }
                this.sourceStreamResultList = new ArrayList<>();
                for (IExecute.VtStream source : sources) {
                    VtStreamResultSet sourceStreamResultSet = new VtStreamResultSet(source, true);
                    if (seenFields == null) {
                        seenFields = sourceStreamResultSet.getFields();
                    } else {
                        compareFields(seenFields, sourceStreamResultSet.getFields());
                    }
                    sourceStreamResultList.add(sourceStreamResultSet);
                }
            }

            @Override
            public void close() throws SQLException {
                this.sourceStreamResultList = null;
                for (IExecute.VtStream source : this.sources) {
                    source.close();
                }
            }
        };
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
            this.compareFields(Arrays.asList(firstQr.getFields()), Arrays.asList(qr.getFields()));
        }
        return firstQr;
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

    private void compareFields(List<Query.Field> fields1, List<Query.Field> fields2) throws SQLException {
        if (fields1.size() != fields2.size()) {
            throw new SQLException("The used SELECT statements have a different number of columns");
        }
        for (int i = 0; i < fields2.size(); i++) {
            Query.Field field1 = fields1.get(i);
            Query.Field field2 = fields2.get(i);
            if (!field1.getType().equals(field2.getType())) {
                throw new SQLException("column field type does not match for name: (%v, %v) types: (%v, %v)");
            }
        }
    }
}
