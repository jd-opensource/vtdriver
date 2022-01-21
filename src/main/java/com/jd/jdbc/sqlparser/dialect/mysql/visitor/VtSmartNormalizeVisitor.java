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

import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLKeep;
import com.jd.jdbc.sqlparser.ast.SQLLimit;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.SQLOver;
import com.jd.jdbc.sqlparser.ast.expr.SQLAggregateExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLAggregateOption;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator;
import com.jd.jdbc.sqlparser.ast.expr.SQLBooleanExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLInListExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLListExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLLiteralExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLMethodInvokeExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNullExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.utils.JdbcConstants;
import com.jd.jdbc.sqlparser.visitor.ExportParameterVisitorUtils;
import com.jd.jdbc.sqlparser.visitor.VisitorFeature;
import com.jd.jdbc.srvtopo.BindVariable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;


public class VtSmartNormalizeVisitor extends MySqlExportParameterVisitor {
    private boolean parameterizedSelectOutput = true;

    private boolean parameterizedMethodOutput = true;

    private boolean parameterizedOrderbyOutput = true;

    private boolean parameterizedGroupbyOutput = true;

    private boolean parameterizedLimitOutput = true;

    public VtSmartNormalizeVisitor(List<Object> parameters, Appendable appender, boolean wantParameterizedOutput) {
        super(parameters, appender, wantParameterizedOutput);
    }

    public void init(Map<String, BindVariable> bindVariableMap) {
        this.setShardingSupport(false);
        if (bindVariableMap == null) {
            this.setInputParameters(new ArrayList<>());
        } else {
            this.setInputParameters(new ArrayList<>(bindVariableMap.values()));
        }
        this.setParameterizedMergeInList(false);
        this.setParameterizedQuesUnMergeInList(true);
        this.setPrettyFormat(false);
        this.setUppCase(false);
        this.setParameterizedGroupbyOutput(false);
        this.setParameterizedOrderbyOutput(false);
        this.config(VisitorFeature.OutputParameterizedQuesUnMergeOr, true);
    }

    public String getQuery() {
        return appender.toString();
    }

    @Override
    public boolean visit(final SQLVariantRefExpr x) {
        int index = x.getIndex();

        if (index < 0 || inputParameters == null || index >= inputParameters.size()) {
            print0(x.getName());
            return false;
        }

        Object param = inputParameters.get(index);

        SQLObject parent = x.getParent();

        boolean in;
        if (parent instanceof SQLInListExpr) {
            in = true;
        } else if (parent instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) parent;
            in = binaryOpExpr.getOperator() == SQLBinaryOperator.Equality;
        } else {
            in = false;
        }

        if (in && param instanceof Collection) {
            boolean first = true;
            for (Object item : (Collection) param) {
                if (!first) {
                    print0(", ");
                }
                printParameter(item);
                first = false;
            }
        } else {
            print('?');
        }
        this.parameters.add(param);
        return false;
    }

    @Override
    public boolean visit(final SQLBooleanExpr x) {
        if (this.parameterized) {
            print('?');
            incrementReplaceCunt();

            if (this.parameters != null) {
                ExportParameterVisitorUtils.exportParameter((this).getParameters(), x);
            }
            return false;
        }
        return super.visit(x);
    }

    @Override
    public boolean visit(final SQLSelectItem x) {
        if (parameterizedSelectOutput) {
            return super.visit(x);
        }

        print0(SQLUtils.toMySqlString(x, SQLUtils.NOT_FORMAT_OPTION));

        return true;
    }

    @Override
    public boolean visit(final SQLAggregateExpr x) {

        String methodName = x.getMethodName();
        print0(ucase ? methodName : methodName.toLowerCase());
        print('(');

        SQLAggregateOption option = x.getOption();
        if (option != null) {
            print0(option.toString());
            print(' ');
        }

        List<SQLExpr> arguments = x.getArguments();
        for (int i = 0, size = arguments.size(); i < size; ++i) {
            if (i != 0) {
                print0(", ");
            }
            printExpr(arguments.get(i));
        }

        visitAggreateRest(x);

        print(')');

        if (!JdbcConstants.POSTGRESQL.equals(dbType)) {
            SQLOrderBy withGroup = x.getWithinGroup();
            if (withGroup != null) {
                print0(ucase ? " WITHIN GROUP (" : " within group (");
                visit(withGroup);
                print(')');
            }
        }

        SQLKeep keep = x.getKeep();
        if (keep != null) {
            print(' ');
            visit(keep);
        }

        SQLOver over = x.getOver();
        if (over != null) {
            print(' ');
            over.accept(this);
        }

        final SQLExpr filter = x.getFilter();
        if (filter != null) {
            print0(ucase ? "FILTER (WHERE " : "filter (where ");
            printExpr(filter);
            print(')');
        }

        return false;
    }

    @Override
    public boolean visit(final SQLLimit x) {
        if (parameterizedLimitOutput) {
            return super.visit(x);
        }

        print0(SQLUtils.toMySqlString(x, SQLUtils.NOT_FORMAT_OPTION));

        return true;
    }

    @Override
    public boolean visit(final SQLOrderBy x) {
        if (parameterizedOrderbyOutput) {
            return super.visit(x);
        }

        print0(SQLUtils.toMySqlString(x, SQLUtils.NOT_FORMAT_OPTION));

        return false;
    }

    @Override
    public boolean visit(final SQLSelectGroupByClause x) {
        if (parameterizedGroupbyOutput) {
            return super.visit(x);
        }

        print0(SQLUtils.toMySqlString(x, SQLUtils.NOT_FORMAT_OPTION));

        return false;
    }

    /**
     * rewrite visit(SQLInListExpr x) for SQLNullExpr
     * add SQLNullExpr checkpoint and remove println function
     *
     * @param x
     * @return
     */
    @Override
    public boolean visit(final SQLInListExpr x) {
        if (this.parameterized) {
            List<SQLExpr> targetList = x.getTargetList();

            boolean allLiteral = true;
            for (SQLExpr item : targetList) {
                if (item instanceof SQLNullExpr) {
                    allLiteral = false;
                    break;
                }

                if (!(item instanceof SQLLiteralExpr)) {
                    if (item instanceof SQLListExpr) {
                        SQLListExpr list = (SQLListExpr) item;
                        for (SQLExpr listItem : list.getItems()) {
                            if (!(listItem instanceof SQLLiteralExpr)) {
                                allLiteral = false;
                                break;
                            }
                        }
                        if (allLiteral) {
                            break;
                        }
                        continue;
                    }
                    allLiteral = false;
                    break;
                }
            }

            if (allLiteral) {
                final boolean changed = targetList.size() != 1 || !(targetList.get(0) instanceof SQLVariantRefExpr);

                printExpr(x.getExpr());

                if (x.isNot()) {
                    print(ucase ? " NOT IN" : " not in");
                } else {
                    print(ucase ? " IN" : " in");
                }

                if (!isParameterizedQuesUnMergeInList() || targetList.size() == 1) {
                    print(" (?)");
                } else {
                    print(" (");
                    for (int i = 0; i < targetList.size(); i++) {
                        if (i == 0) {
                            print("?");
                        } else {
                            print(", ?");
                        }
                    }
                    print(")");
                }

                if (changed) {
                    incrementReplaceCunt();
                    if (this.parameters != null) {
                        if (parameterizedMergeInList) {
                            List<Object> subList = new ArrayList<Object>(x.getTargetList().size());
                            for (SQLExpr target : x.getTargetList()) {
                                ExportParameterVisitorUtils.exportParameter(subList, target);
                            }
                            parameters.add(subList);
                        } else {
                            for (SQLExpr target : x.getTargetList()) {
                                ExportParameterVisitorUtils.exportParameter(this.parameters, target);
                            }
                        }
                    }
                }

                return false;
            }
        }

        printExpr(x.getExpr());

        if (x.isNot()) {
            print0(ucase ? " NOT IN (" : " not in (");
        } else {
            print0(ucase ? " IN (" : " in (");
        }

        List<SQLExpr> targetList = x.getTargetList();
        for (int i = 0; i < targetList.size(); i++) {
            if (i != 0) {
                print0(", ");
            }
            printExpr(targetList.get(i));
        }

        print(')');
        return false;
    }

    @Override
    public boolean visit(final SQLMethodInvokeExpr x) {
        if (parameterizedMethodOutput) {
            return super.visit(x);
        }

        print0(SQLUtils.toMySqlString(x, SQLUtils.NOT_FORMAT_OPTION));

        return true;
    }

    @Override
    public boolean visit(final SQLIdentifierExpr x) {
        x.setName(x.normalizedName());
        return super.visit(x);
    }

    public void setParameterizedSelectOutput(final boolean parameterizedSelectOutput) {
        this.parameterizedSelectOutput = parameterizedSelectOutput;
    }

    public void setParameterizedMethodOutput(final boolean parameterizedMethodOutput) {
        this.parameterizedMethodOutput = parameterizedMethodOutput;
    }

    public void setParameterizedOrderbyOutput(final boolean parameterizedOrderbyOutput) {
        this.parameterizedOrderbyOutput = parameterizedOrderbyOutput;
    }

    public void setParameterizedGroupbyOutput(final boolean parameterizedGroupbyOutput) {
        this.parameterizedGroupbyOutput = parameterizedGroupbyOutput;
    }

    public void setParameterizedLimitOutput(final boolean parameterizedLimitOutput) {
        this.parameterizedLimitOutput = parameterizedLimitOutput;
    }
}
