/*
Copyright 2021 JD Project Authors.

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

package com.jd.jdbc.sqlparser.dialect.mysql.visitor;

import com.jd.jdbc.sqlparser.ast.expr.SQLAggregateExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLExistsExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLInSubQueryExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSubqueryTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQuery;
import java.util.Set;
import lombok.Setter;

/**
 * <p>
 * used to check if this AST contains specific node types
 */
@Setter
public class CheckNodeTypesVisitor extends MySqlASTVisitorAdapter {

    private Set<CheckNodeType> nodeType;

    private boolean checkResult;

    public CheckNodeTypesVisitor(final Set<CheckNodeType> nodeType) {
        this.nodeType = nodeType;
        this.checkResult = false;
    }

    public boolean getCheckResult() {
        return checkResult;
    }

    @Override
    public boolean visit(final SQLAggregateExpr x) {
        if (this.nodeType.contains(CheckNodeType.AGGREGATE)) {
            this.checkResult = true;
            return false;
        }
        return true;
    }

    @Override
    public boolean visit(final SQLUnionQuery x) {
        if (this.nodeType.contains(CheckNodeType.UNION)) {
            this.checkResult = true;
            return false;
        }
        return true;
    }

    @Override
    public boolean visit(final SQLSubqueryTableSource x) {
        if (this.nodeType.contains(CheckNodeType.SUBQUERY)) {
            this.checkResult = true;
            return false;
        }

        return true;
    }

    @Override
    public boolean visit(final SQLInSubQueryExpr x) {
        if (this.nodeType.contains(CheckNodeType.SUBQUERY)) {
            this.checkResult = true;
            return false;
        }
        return true;
    }

    @Override
    public boolean visit(final SQLExistsExpr x) {
        if (this.nodeType.contains(CheckNodeType.SUBQUERY)) {
            this.checkResult = true;
            return false;
        }
        return true;
    }

    public enum CheckNodeType {
        AGGREGATE,
        UNION,
        SUBQUERY
    }
}
