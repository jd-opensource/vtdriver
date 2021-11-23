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

import java.util.Objects;
import lombok.Data;

@Data
public class ResultColumn {
    // alias will represent the unqualified symbol name for that expression.
    // If the statement provides an explicit alias, that name will be used.
    // If the expression is a simple column, then the base name of the
    // column will be used as the alias. If the expression is non-trivial,
    // alias will be empty, and cannot be referenced from other parts of
    // the query.
    //在Vitess里面，为sqlparser.ColIdent, 在Druid里面, 暂时用SQLIdentifierExpr代替
    private String alias;

    private Column column;

    public ResultColumn() {
    }

    public ResultColumn(Column column) {
        this.column = column;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ResultColumn)) {
            return false;
        }
        ResultColumn that = (ResultColumn) o;
        return Objects.equals(alias, that.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(alias);
    }

    @Override
    public String toString() {
        return "ResultColumn{" +
            "alias='" + alias + '\'' +
            '}';
    }
}
