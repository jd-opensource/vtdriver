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

package com.jd.jdbc.planbuilder.gen4;

import java.util.Iterator;
import java.util.List;
import lombok.Getter;

public class SortedIterator implements Iterator<SortedIterator> {
    private List<QueryProjection.GroupBy> grouping;

    private List<QueryProjection.Aggr> aggregations;

    @Getter
    private QueryProjection.GroupBy valueGB;

    @Getter
    private QueryProjection.Aggr valueA;

    private int groupbyIdx;

    private int aggrIdx;

    public SortedIterator(List<QueryProjection.GroupBy> grouping, List<QueryProjection.Aggr> aggregations) {
        this.grouping = grouping;
        this.aggregations = aggregations;
    }

    @Override
    public boolean hasNext() {
        if (this.aggrIdx < aggregations.size() && this.groupbyIdx < grouping.size()) {
            QueryProjection.Aggr aggregation = aggregations.get(aggrIdx);
            QueryProjection.GroupBy groupBy = grouping.get(groupbyIdx);
            if (compareRefInt(aggregation.getIndex(), groupBy.getInnerIndex())) {
                aggrIdx++;
                valueA = aggregation;
                valueGB = null;
                return true;
            }
            groupbyIdx++;
            valueA = null;
            valueGB = groupBy;
            return true;
        }
        if (this.groupbyIdx < grouping.size()) {
            QueryProjection.GroupBy groupBy = grouping.get(groupbyIdx);
            groupbyIdx++;
            valueA = null;
            valueGB = groupBy;
            return true;
        }
        if (this.aggrIdx < aggregations.size()) {
            QueryProjection.Aggr aggregation = aggregations.get(aggrIdx);
            aggrIdx++;
            valueA = aggregation;
            valueGB = null;
            return true;
        }
        return false;
    }

    @Override
    public SortedIterator next() {
        return this;
    }

    private boolean compareRefInt(Integer a, Integer b) {
        if (a == null) {
            return false;
        } else if (b == null) {
            return true;
        }
        return (b - a) > 0;
    }
}
