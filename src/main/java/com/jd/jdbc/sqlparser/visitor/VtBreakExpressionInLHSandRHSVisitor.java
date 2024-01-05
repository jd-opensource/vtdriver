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

package com.jd.jdbc.sqlparser.visitor;

import com.jd.jdbc.planbuilder.semantics.SemTable;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefExpr;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

@Getter
public class VtBreakExpressionInLHSandRHSVisitor extends MySqlASTVisitorAdapter {

    private TableSet lhs;

    private SemTable semTable;

    private String errMsg;

    private Boolean bErr;

    private List<String> bvNames;

    private List<SQLName> columus;

    public VtBreakExpressionInLHSandRHSVisitor(TableSet lhs, SemTable semTable) {
        this.lhs = lhs;
        this.semTable = semTable;
        this.errMsg = "";
        this.bErr = false;
        this.bvNames = new ArrayList<>();
        this.columus = new ArrayList<>();
    }

    public boolean visit(SQLIdentifierExpr node) {
        return this.visitColName(node);
    }

    public boolean visit(SQLPropertyExpr node) {
        return this.visitColName(node);
    }

    private boolean visitColName(SQLName node) {
        TableSet deps = this.semTable.recursiveDeps(node);
        if (deps.numberOfTables() == 0) {
            this.errMsg = "unknown column. has the AST been copied? :" + node.getSimpleName();
            this.bErr = true;
            return false;
        }

        if (deps.isSolvedBy(this.lhs)) {
            this.columus.add(node);
            String bvName = node.getSimpleName();
            if (node instanceof SQLIdentifierExpr) {
                bvName = ((SQLIdentifierExpr) node).getName();
            } else if (node instanceof SQLPropertyExpr) {
                bvName = ((SQLPropertyExpr) node).getOwnernName() + "_" + ((SQLPropertyExpr) node).getName();
            }
            this.bvNames.add(bvName);
            SQLVariantRefExpr arg = new SQLVariantRefExpr(":" + bvName);
            // we are replacing one of the sides of the comparison with an argument,
            // but we don't want to lose the type information we have, so we copy it over
            this.semTable.copyExprInfo(node, arg);
            SQLUtils.replaceInParent(node, arg);
        }
        return false;
    }
}
