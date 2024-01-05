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

package com.jd.jdbc.planbuilder.semantics;

import com.jd.jdbc.sqlparser.ast.SQLObject;
import java.util.Objects;
import java.util.StringJoiner;

public class SQLObjectExpr {
    private SQLObject sqlObject;

    private final int identityHashCode;

    public SQLObjectExpr(SQLObject sqlObject) {
        this.sqlObject = sqlObject;
        this.identityHashCode = System.identityHashCode(sqlObject);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SQLObjectExpr that = (SQLObjectExpr) o;
        return identityHashCode == that.identityHashCode && Objects.equals(sqlObject, that.sqlObject);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sqlObject, identityHashCode);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", SQLObjectExpr.class.getSimpleName() + "[", "]")
            .add("sqlObject=" + sqlObject)
            .add("identityHashCode=" + identityHashCode)
            .toString();
    }
}
