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

package com.jd.jdbc.planbuilder.gen4.operator.physical;

import com.jd.jdbc.planbuilder.gen4.IntroducesTable;
import com.jd.jdbc.planbuilder.gen4.QueryTable;
import com.jd.jdbc.planbuilder.semantics.SemTable;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import vschema.Vschema;

@Getter
@Setter
public class Table implements PhysicalOperator, IntroducesTable {
    private QueryTable qTable;

    private Vschema.Table vTable;

    private List<SQLName> columns;

    public Table() {
        this.columns = new ArrayList<>();
    }

    public Table(QueryTable qTable, Vschema.Table vTable, List<SQLName> columns) {
        this.qTable = qTable;
        this.vTable = vTable;
        this.columns = columns;
    }

    @Override
    public TableSet tableID() {
        return qTable.getId();
    }

    @Override
    public SQLSelectItem unsolvedPredicates(SemTable semTable) {
        return null;
    }

    @Override
    public void checkValid() throws SQLException {

    }

    @Override
    public Integer cost() {
        return 0;
    }

    @Override
    public PhysicalOperator clone() {
        List<SQLName> columns = new ArrayList<>(this.columns.size());
        for (SQLName name : this.columns) {
            columns.add(name.clone());
        }
        return new Table(this.qTable, this.vTable, columns);
    }
}
