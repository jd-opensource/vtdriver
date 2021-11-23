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

package com.jd.jdbc.planbuilder;

import com.jd.jdbc.tindexes.TableIndex;
import com.jd.jdbc.vindexes.hash.BinaryHash;
import io.vitess.proto.Query;
import lombok.Getter;
import lombok.Setter;

// column represents a unique symbol in the query that other
// parts can refer to. If a column originates from a sharded
// table, and is tied to a vindex, then its Vindex field is
// set, which can be used to improve a route's plan.
// Every column contains the builder it originates from.
//
// Two columns are equal if their pointer values match.
//
// For subquery and vindexFunc, the colnum is also set because
// the column order is known and unchangeable.

public class Column {
    private final Builder origin;

    //    private Vindexes.Vindex vindex;
    @Getter
    @Setter
    private BinaryHash vindex;

    @Setter
    @Getter
    private TableIndex tindex;

    @Setter
    @Getter
    private Symtab st;

    @Getter
    private Query.Type type = Query.Type.NULL_TYPE;

    @Getter
    @Setter
    private Integer colNumber;

    public Column(Builder origin) {
        this.origin = origin;
    }

    public Column(Builder origin, Symtab st) {
        this(origin);
        this.st = st;
    }

    public Column(Builder origin, Symtab st, Query.Type type) {
        this(origin, st);
        if (type != null && type != Query.Type.NULL_TYPE) {
            this.type = type;
        }
    }

    public Column(Builder origin, Symtab st, Query.Type type, Integer colNumber) {
        this(origin, st, type);
        this.colNumber = colNumber;
    }

    public Builder origin() {
        // If it's a route, we have to resolve it.
        if (this.origin instanceof RoutePlan) {
            return ((RoutePlan) this.origin).resolve();
        }
        return this.origin;
    }
}
