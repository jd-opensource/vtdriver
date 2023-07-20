/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jd.jdbc.sqlparser.dialect.mysql.visitor;

import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLLimit;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.expr.SQLAggregateExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBetweenExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator;
import com.jd.jdbc.sqlparser.ast.expr.SQLBooleanExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLCaseExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLCastExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLCharExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLDateExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLInListExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntegerExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntervalExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLMethodInvokeExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNCharExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNullExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNumberExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLTimestampExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLUnaryExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLInsertStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLUpdateSetItem;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.expr.MySqlCharExpr;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;


public class VtReplaceVariantRefExprVisitor extends MySqlASTVisitorAdapter {
    @Getter
    private final List<SQLVariantRefExpr> varRefList = new ArrayList<>();

    private int refIndex = -1;

    @Override
    public boolean visit(final SQLOrderBy x) {
        return false;
    }

    @Override
    public boolean visit(final SQLSelectGroupByClause x) {
        return false;
    }

    @Override
    public boolean visit(final SQLMethodInvokeExpr x) {
        String function = x.getMethodName();
        List<SQLExpr> parameters = x.getParameters();

        for (int i = 0, size = parameters.size(); i < size; ++i) {
            SQLExpr param = parameters.get(i);
            if (size == 2 && i == 1 && param instanceof SQLCharExpr) {
                if ("DATE_FORMAT".equalsIgnoreCase(function)) {
                    continue;
                }
            }

            param.accept(this);
        }

        return false;
    }

    @Override
    public boolean visit(final SQLVariantRefExpr x) {
        if (refIndex < x.getIndex()) {
            refIndex = x.getIndex();
        } else {
            x.setIndex(++refIndex);
        }
        this.varRefList.add(x);
        return false;
    }

    @Override
    public boolean visit(final SQLAggregateExpr x) {
        visitChilds(x.getArguments());
        visitChild(x.getKeep());
        visitChild(x.getOver());
        visitChild(x.getOverRef());
        visitChild(x.getWithinGroup());
        this.visitAggreateRest(x);
        return false;
    }

    @Override
    public boolean visit(final SQLIntegerExpr x) {
        SQLObject parent = x.getParent();
        if (parent instanceof SQLBinaryOpExpr) {
            SQLExpr left = ((SQLBinaryOpExpr) parent).getLeft();
            SQLExpr right = ((SQLBinaryOpExpr) parent).getRight();
            SQLBinaryOperator operator = ((SQLBinaryOpExpr) parent).getOperator();
            if (operator.isRelational()
                && left instanceof SQLIntegerExpr
                && right instanceof SQLIntegerExpr) {
                return false;
            }
        }
        this.rewrite(x);
        return false;
    }

    @Override
    public boolean visit(final SQLCharExpr x) {
        this.rewrite(x);
        return false;
    }

    @Override
    public boolean visit(final MySqlCharExpr x) {
        this.rewrite(x);
        return false;
    }

    @Override
    public boolean visit(final SQLNCharExpr x) {
        this.rewrite(x);
        return false;
    }

    @Override
    public boolean visit(final SQLBooleanExpr x) {
        this.rewrite(x);
        return false;
    }

    @Override
    public boolean visit(final SQLNumberExpr x) {
        this.rewrite(x);
        return false;
    }

    @Override
    public boolean visit(final SQLTimestampExpr x) {
        this.rewrite(x);
        return false;
    }

    @Override
    public boolean visit(final SQLDateExpr x) {
        this.rewrite(x);
        return false;
    }

    @Override
    public boolean visit(final SQLCastExpr x) {
        x.getExpr().accept(this);
        return false;
    }

    @Override
    public boolean visit(final SQLNullExpr x) {
        if (x.getParent() instanceof SQLInsertStatement.ValuesClause) {
            this.rewrite(x);
        }
        return false;
    }

    @Override
    public boolean visit(final SQLIntervalExpr x) {
        SQLExpr expr = x.getValue();
        if (expr.getParent() == null) {
            expr.setParent(x);
        }
        expr.accept(this);
        return false;
    }

    private void visitChilds(List<? extends SQLObject> objs) {
        if (objs == null) {
            return;
        }
        for (SQLObject obj : objs) {
            visitChild(obj);
        }
    }

    private void visitChild(SQLObject obj) {
        if (obj == null) {
            return;
        }
        obj.accept(this);
    }

    private void visitAggreateRest(final SQLAggregateExpr x) {
        {
            SQLOrderBy value = (SQLOrderBy) x.getAttribute("ORDER BY");
            if (value != null) {
                value.accept(this);
            }
        }
        {
            Object value = x.getAttribute("SEPARATOR");
            if (value != null) {
                ((SQLObject) value).accept(this);
            }
        }
    }

    private void rewrite(final SQLObject x) {
        SQLObject parent = x.getParent();

        if (parent instanceof SQLBinaryOpExpr) {
            SQLBinaryOperator operator = ((SQLBinaryOpExpr) parent).getOperator();
            SQLExpr left = ((SQLBinaryOpExpr) parent).getLeft();
            SQLExpr right = ((SQLBinaryOpExpr) parent).getRight();
            if (operator.isRelational()
                && left instanceof SQLIntegerExpr
                && right instanceof SQLIntegerExpr) {
                return;
            }
            if (left == x) {
                ((SQLBinaryOpExpr) parent).setLeft(this.newVariantRefExpr(parent));
            } else {
                ((SQLBinaryOpExpr) parent).setRight(this.newVariantRefExpr(parent));
            }
        } else if (parent instanceof SQLUnaryExpr) {
            ((SQLUnaryExpr) parent).setExpr(this.newVariantRefExpr(parent));
        } else if (parent instanceof SQLInListExpr) {
            List<SQLExpr> targetList = ((SQLInListExpr) parent).getTargetList();
            int replaceIndex = -1;
            for (int i = 0; i < targetList.size(); i++) {
                SQLExpr expr = targetList.get(i);
                if (expr instanceof SQLVariantRefExpr) {
                    continue;
                }
                if (expr == x) {
                    replaceIndex = i;
                    break;
                }
            }
            targetList.set(replaceIndex, this.newVariantRefExpr(parent));
        } else if (parent instanceof SQLLimit) {
            if (((SQLLimit) parent).getOffset() == x) {
                ((SQLLimit) parent).setOffset(this.newVariantRefExpr(parent));
            } else {
                ((SQLLimit) parent).setRowCount(this.newVariantRefExpr(parent));
            }
        } else if (parent instanceof SQLMethodInvokeExpr) {
            List<SQLExpr> parameters = ((SQLMethodInvokeExpr) parent).getParameters();
            int replaceIndex = -1;
            for (int i = 0; i < parameters.size(); i++) {
                SQLExpr expr = parameters.get(i);
                if (expr instanceof SQLVariantRefExpr) {
                    continue;
                }
                if (expr == x) {
                    replaceIndex = i;
                    if (x instanceof SQLIntervalExpr) {
                        ((SQLIntervalExpr) x).setValue(this.newVariantRefExpr(x.getParent()));
                        return;
                    }
                    break;
                }
            }
            parameters.set(replaceIndex, this.newVariantRefExpr(parent));
        } else if (parent instanceof SQLUpdateSetItem) {
            ((SQLUpdateSetItem) parent).setValue(this.newVariantRefExpr(x.getParent()));
        } else if (parent instanceof SQLCaseExpr) {
            if (((SQLCaseExpr) parent).getValueExpr() == x) {
                ((SQLCaseExpr) parent).setValueExpr(this.newVariantRefExpr(parent));
            } else {
                ((SQLCaseExpr) parent).setElseExpr(this.newVariantRefExpr(parent));
            }
        } else if (parent instanceof SQLCaseExpr.Item) {
            if (((SQLCaseExpr.Item) parent).getConditionExpr() == x) {
                ((SQLCaseExpr.Item) parent).setConditionExpr(this.newVariantRefExpr(parent));
            } else {
                ((SQLCaseExpr.Item) parent).setValueExpr(this.newVariantRefExpr(parent));
            }
        } else if (parent instanceof SQLSelectItem) {
            ((SQLSelectItem) parent).setExpr(this.newVariantRefExpr(parent));
        } else if (parent instanceof SQLCastExpr) {
            ((SQLCastExpr) parent).setExpr(this.newVariantRefExpr(parent));
        } else if (parent instanceof SQLInsertStatement.ValuesClause) {
            List<SQLExpr> valueList = ((SQLInsertStatement.ValuesClause) parent).getValues();
            int replaceIndex = -1;
            for (int i = 0; i < valueList.size(); i++) {
                SQLExpr expr = valueList.get(i);
                if (expr instanceof SQLVariantRefExpr) {
                    continue;
                }
                if (expr == x) {
                    replaceIndex = i;
                    break;
                }
            }
            valueList.set(replaceIndex, this.newVariantRefExpr(parent));
        } else if (parent instanceof SQLBetweenExpr) {
            if (((SQLBetweenExpr) parent).getBeginExpr() == x) {
                ((SQLBetweenExpr) parent).setBeginExpr(this.newVariantRefExpr(parent));
            } else {
                ((SQLBetweenExpr) parent).setEndExpr(this.newVariantRefExpr(parent));
            }
        } else if (parent instanceof MySqlSelectQueryBlock) {
            if (((MySqlSelectQueryBlock) parent).getWhere() == x) {
                ((MySqlSelectQueryBlock) parent).setWhere(this.newVariantRefExpr(parent));
            }
        } else if (parent instanceof SQLIntervalExpr) {
            if (((SQLIntervalExpr) parent).getValue() == x) {
                ((SQLIntervalExpr) parent).setValue(this.newVariantRefExpr(parent));
            }
        } else if (parent instanceof SQLAggregateExpr) {
            replaceAggregateChild(x, (SQLAggregateExpr) parent);
        }
    }

    private void replaceAggregateChild(SQLObject x, SQLAggregateExpr parent) {
        List<SQLExpr> parameters = parent.getArguments();
        int replaceIndex = -1;
        for (int i = 0; i < parameters.size(); i++) {
            SQLExpr expr = parameters.get(i);
            if (expr instanceof SQLVariantRefExpr) {
                continue;
            }
            if (expr == x) {
                replaceIndex = i;
                break;
            }
        }
        if (replaceIndex != -1) {
            parameters.set(replaceIndex, this.newVariantRefExpr(parent));
            return;
        }

        if (parent.getAttribute("SEPARATOR") == x) {
            parent.getAttributes().put("SEPARATOR", this.newVariantRefExpr(parent));
        }
    }

    /**
     * @param parent
     * @return
     */
    private SQLVariantRefExpr newVariantRefExpr(final SQLObject parent) {
        SQLVariantRefExpr variantRefExpr = new SQLVariantRefExpr("?");
        if (parent != null) {
            variantRefExpr.setParent(parent);
        }
        this.varRefList.add(variantRefExpr);
        variantRefExpr.setIndex(++refIndex);
        return variantRefExpr;
    }
}
