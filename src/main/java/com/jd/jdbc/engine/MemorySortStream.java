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

import com.google.common.collect.MinMaxPriorityQueue;
import com.jd.jdbc.IExecute;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtRowList;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import lombok.Getter;

public class MemorySortStream implements IExecute.VtStream {
    private IExecute.VtStream stream = null;

    private int truncate = 0;

    private int expectCount = 0;

    private List<OrderByParams> orderBy = null;

    private Query.Field[] fields = null;

    private boolean fetched = false;

    private List<List<VtResultValue>> sortedRows = null;

    public MemorySortStream(IExecute.VtStream stream, List<OrderByParams> orderBy, int truncate, int expectCount) {
        this.stream = stream;
        this.orderBy = orderBy;
        this.truncate = truncate;
        this.expectCount = expectCount;
    }

    private void fetchAndSort(boolean wantFields) throws SQLException {
        StreamRowComparator comparator = new StreamRowComparator(orderBy);
        MinMaxPriorityQueue<List<VtResultValue>> priorityQueue = MinMaxPriorityQueue
            .orderedBy(comparator)
            .maximumSize(expectCount)
            .create();

        while (true) {
            VtResultSet fetched = (VtResultSet) stream.fetch(wantFields);
            if (fields == null) {
                fields = fetched.getFields();
            }

            if (!fetched.hasNext()) {
                break;
            }

            while (fetched.hasNext()) {
                priorityQueue.add(fetched.next());
            }
        }

        if (comparator.getException() != null) {
            throw comparator.getException();
        }

        sortedRows = new ArrayList<>();
        while (!priorityQueue.isEmpty()) {
            sortedRows.add(priorityQueue.poll());
        }
    }

    @Override
    public VtRowList fetch(boolean wantFields) throws SQLException {
        VtResultSet vtResultSet = new VtResultSet();
        if (stream == null || fetched) {
            // fake stream
            return vtResultSet;
        }

        fetchAndSort(wantFields);

        if (wantFields) {
            vtResultSet.setFields(fields);
        }
        vtResultSet.setRows(sortedRows);
        vtResultSet.setRowsAffected(vtResultSet.getRows().size());
        fetched = true;
        return vtResultSet.truncate(truncate);
    }

    @Override
    public void close() throws SQLException {
        if (stream != null) {
            stream.close();
            stream = null;
        }
    }

    class StreamRowComparator implements Comparator<List<VtResultValue>> {
        private final List<OrderByParams> orderBy;

        @Getter
        private SQLException exception;

        public StreamRowComparator(List<OrderByParams> orderBy) {
            this.exception = null;
            this.orderBy = orderBy;
        }

        @Override
        public int compare(List<VtResultValue> o1, List<VtResultValue> o2) {
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
                    cmp = EvalEngine.nullSafeCompare(o1.get(order.col), o2.get(order.col));
                } catch (SQLException e) {
                    this.exception = e;
                    return -1;
                }
                if (cmp == 0) {
                    continue;
                }
                if (order.desc) {
                    return -cmp;
                }
                return cmp;
            }
            return 0;
        }
    }
}
