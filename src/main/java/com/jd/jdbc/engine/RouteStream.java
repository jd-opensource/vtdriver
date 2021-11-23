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
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.queryservice.StreamIterator;
import com.jd.jdbc.queryservice.VtIterator;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtRowList;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import lombok.AllArgsConstructor;
import lombok.Getter;

public class RouteStream implements IExecute.VtStream {
    private final List<StreamIterator> iterators;

    private final List<OrderByParams> orderBy;

    private final StreamRowComparator comparator;

    private final RouteEngine routeEngine;

    private final Map<String, Query.BindVariable> bindVariableMap;

    Vcursor vcursor;

    private int truncate = 0;

    private Query.Field[] fields = null;

    //for sort fetch
    private int nextFetchShardIndex = -1;

    private PriorityQueue<RowWithShard> sortedValues = null;

    public RouteStream(List<StreamIterator> iterators, List<OrderByParams> orderBy, int truncate, RouteEngine routeEngine, Vcursor vcursor, Map<String, Query.BindVariable> bindVariableMap) {
        this.iterators = iterators;
        this.orderBy = orderBy;
        this.comparator = new StreamRowComparator(orderBy);
        this.truncate = truncate;
        this.routeEngine = routeEngine;
        this.vcursor = vcursor;
        this.bindVariableMap = bindVariableMap;
    }

    @Override
    public VtRowList fetch(boolean wantFields) throws SQLException {
        VtResultSet vtResultSet = new VtResultSet();
        if (iterators == null) {
            return vtResultSet;
        }

        if (this.orderBy == null || this.orderBy.isEmpty()) {
            for (VtIterator<VtResultSet> it : iterators) {
                if (it.hasNext()) {
                    VtResultSet next = it.next();
                    vtResultSet.appendResult(next);
                    if (fields == null) {
                        fields = vtResultSet.getFields();
                    }
                }
            }

            vtResultSet.setRowsAffected(vtResultSet.getRows() == null ? 0 : vtResultSet.getRows().size());

            if (wantFields && vtResultSet.getFields() == null) {
                vtResultSet.appendResult(this.routeEngine.getFields(vcursor, bindVariableMap));
                fields = vtResultSet.getFields();
            }
            return vtResultSet.truncate(this.truncate);
        }

        return fetchOrdered(wantFields);
    }

    @Override
    public void close() throws SQLException {
        if (iterators == null || iterators.isEmpty()) {
            return;
        }
        for (VtIterator<VtResultSet> it : iterators) {
            StreamIterator streamIterator = (StreamIterator) it;
            streamIterator.close();
        }
        iterators.clear();
    }

    //rewrite if necessary. Time complexity = O(logN), N: shard count;
    private VtRowList fetchOrdered(boolean wantFields) throws SQLException {
        //first fetch
        if (nextFetchShardIndex == -1) {
            sortedValues = new PriorityQueue<>(comparator);
            VtIterator<VtResultSet> it;
            for (int idx = 0; idx < iterators.size(); idx++) {
                it = iterators.get(idx);
                if (it.hasNext()) {
                    VtResultSet tmpResultSet = it.next();
                    if (fields == null) {
                        fields = tmpResultSet.getFields();
                    }
                    while (tmpResultSet.hasNext()) {
                        sortedValues.offer(new RowWithShard(idx, tmpResultSet.next()));
                    }
                }
            }
        } else {
            VtIterator<VtResultSet> it = iterators.get(nextFetchShardIndex);
            if (it.hasNext()) {
                VtResultSet tmpResultSet = it.next();
                while (tmpResultSet.hasNext()) {
                    sortedValues.offer(new RowWithShard(nextFetchShardIndex, tmpResultSet.next()));
                }
            }
        }

        if (comparator.getException() != null) {
            throw comparator.getException();
        }

        VtResultSet vtResultSet = new VtResultSet();
        if (wantFields && vtResultSet.getFields() == null) {
            vtResultSet.appendResult(this.routeEngine.getFields(vcursor, bindVariableMap));
            fields = vtResultSet.getFields();
        }

        if (sortedValues.isEmpty()) {
            return vtResultSet;
        }

        RowWithShard poll = sortedValues.poll();
        vtResultSet.setRows(Arrays.asList(poll.getRow()));
        vtResultSet.setRowsAffected(vtResultSet.getRows() == null ? 0 : vtResultSet.getRows().size());
        nextFetchShardIndex = poll.getShardIndex();

        return vtResultSet.truncate(this.truncate);
    }

    @AllArgsConstructor
    @Getter
    public class RowWithShard {
        private final int shardIndex;

        private final List<VtResultValue> row;
    }

    class StreamRowComparator implements Comparator<RowWithShard> {
        @Getter
        private SQLException exception;

        private List<OrderByParams> orderBy = null;

        public StreamRowComparator(List<OrderByParams> orderBy) {
            this.orderBy = orderBy;
        }

        @Override
        public int compare(RowWithShard o1, RowWithShard o2) {
            // If there are any errors below, the function sets
            // the external err and returns true. Once err is set,
            // all subsequent calls return true. This will make
            // Slice think that all elements are in the correct
            // order and return more quickly.
            for (OrderByParams order : orderBy) {
                if (exception != null) {
                    return -1;
                }
                Integer cmp;
                try {
                    cmp = EvalEngine.nullSafeCompare(o1.getRow().get(order.col), o2.getRow().get(order.col));
                } catch (SQLException e) {
                    this.exception = e;
                    return -1;
                }
                if (cmp == 0) {
                    continue;
                }
                if (order.desc) {
                    cmp = -cmp;
                }
                return cmp;
            }
            return 0;
        }
    }
}
