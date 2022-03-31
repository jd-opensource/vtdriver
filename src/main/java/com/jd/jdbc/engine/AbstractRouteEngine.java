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

import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqltypes.VtPlanValue;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.vindexes.VKeyspace;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import lombok.Data;
import lombok.Getter;

@Data
public abstract class AbstractRouteEngine implements PrimitiveEngine {

    protected VKeyspace keyspace;

    protected String tableName = "";

    protected SQLSelectQuery selectQuery;

    /**
     * TruncateColumnCount specifies the number of columns to return
     * in the final result. Rest of the columns are truncated
     * from the result received. If 0, no truncation happens.
     */
    protected Integer truncateColumnCount = 0;

    /**
     * OrderBy specifies the key order for merge sorting. This will be
     * set only for scatter queries that need the results to be
     * merge-sorted.
     */
    protected List<OrderByParams> orderBy = new ArrayList<>();

    /**
     * Values specifies the vindex values to use for routing.
     */
    protected List<VtPlanValue> vtPlanValueList = new ArrayList<>();

    @Override
    public String getKeyspaceName() {
        return this.keyspace.getName();
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }

    protected static class VtResultComparator implements Comparator<List<VtResultValue>> {
        private final List<OrderByParams> orderBy;

        @Getter
        private SQLException exception;

        public VtResultComparator(List<OrderByParams> orderBy) {
            this.orderBy = orderBy;
            this.exception = null;
        }

        @Override
        public int compare(List<VtResultValue> o1, List<VtResultValue> o2) {
            // If there are any errors below, the function sets
            // the external err and returns true. Once err is set,
            // all subsequent calls return true. This will make
            // Slice think that all elements are in the correct
            // order and return more quickly.
            for (OrderByParams order : this.orderBy) {
                if (exception != null) {
                    return -1;
                }
                Integer cmp;
                try {
                    cmp = EvalEngine.nullSafeCompare(o1.get(order.getCol()), o2.get(order.getCol()));
                } catch (SQLException e) {
                    this.exception = e;
                    return -1;
                }
                if (cmp == 0) {
                    continue;
                }
                if (order.getDesc()) {
                    cmp = -cmp;
                }
                return cmp;
            }
            return 0;
        }
    }
}
