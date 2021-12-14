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

import com.jd.jdbc.engine.InsertEngine;
import com.jd.jdbc.engine.LimitEngine;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLLimit;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.expr.SQLBetweenExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBooleanExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLCaseExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLCastExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLCharExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLDateExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLHexExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLInListExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntegerExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntervalExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLMethodInvokeExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNullExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNumberExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLTimestampExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLValuableExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLInsertStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLUpdateSetItem;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.srvtopo.BindVariable;
import io.vitess.proto.Query;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.binary.Hex;


public class VtPutBindVarsVisitor extends MySqlASTVisitorAdapter {
    private static final Log LOGGER = LogFactory.getLog(VtPutBindVarsVisitor.class);

    private static final Integer DEFAULT_VAR_REFINDEX = -1;

    private final Map<String, BindVariable> bindVariableMap;

    public VtPutBindVarsVisitor(final Map<String, BindVariable> bindVariableMap) {
        this.bindVariableMap = bindVariableMap;
    }

    @Override
    public boolean visit(final SQLInListExpr x) {
        List<SQLExpr> targetList = x.getTargetList();
        if (targetList.size() != 1) {
            return true;
        } else if (!(targetList.get(0) instanceof SQLVariantRefExpr)) {
            return true;
        }
        List<SQLExpr> newTargetList = new ArrayList<>();
        for (SQLExpr expr : targetList) {
            if (expr instanceof SQLVariantRefExpr) {
                List<SQLValuableExpr> valueableExprList = this.getValueableExprList((SQLVariantRefExpr) expr);
                newTargetList.addAll(valueableExprList);
            } else {
                newTargetList.add(x.clone());
            }
        }
        if (!newTargetList.isEmpty()) {
            targetList.clear();
            targetList.addAll(newTargetList);
        }
        return false;
    }

    @Override
    public boolean visit(final SQLVariantRefExpr x) {
        List<SQLValuableExpr> valueableExprList = this.getValueableExprList(x);

        SQLValuableExpr valueableExpr = null;
        if (valueableExprList.size() == 1) {
            valueableExpr = valueableExprList.get(0);
        }

        SQLObject parent = x.getParent();
        if (parent instanceof SQLBinaryOpExpr) {
            SQLExpr left = ((SQLBinaryOpExpr) parent).getLeft();
            if (left == x) {
                ((SQLBinaryOpExpr) parent).setLeft(valueableExpr);
            } else {
                ((SQLBinaryOpExpr) parent).setRight(valueableExpr);
            }
        } else if (parent instanceof SQLInListExpr) {
            List<SQLExpr> targetList = ((SQLInListExpr) parent).getTargetList();
            int replaceIndex = -1;
            for (int i = 0; i < targetList.size(); i++) {
                SQLExpr expr = targetList.get(i);
                if (!(expr instanceof SQLVariantRefExpr)) {
                    continue;
                }
                SQLVariantRefExpr variantRefExpr = (SQLVariantRefExpr) expr;
                if (variantRefExpr.getIndex() == x.getIndex()) {
                    replaceIndex = i;
                    break;
                }
            }
            targetList.set(replaceIndex, valueableExpr);
        } else if (parent instanceof SQLMethodInvokeExpr) {
            List<SQLExpr> parameters = ((SQLMethodInvokeExpr) parent).getParameters();
            int replaceIndex = -1;
            for (int i = 0; i < parameters.size(); i++) {
                SQLExpr expr = parameters.get(i);
                if (!(expr instanceof SQLVariantRefExpr)) {
                    continue;
                }
                SQLVariantRefExpr variantRefExpr = (SQLVariantRefExpr) expr;
                if (variantRefExpr.getIndex() == x.getIndex()) {
                    replaceIndex = i;
                    break;
                }
            }
            parameters.set(replaceIndex, valueableExpr);
        } else if (parent instanceof SQLCaseExpr) {
            if (((SQLCaseExpr) parent).getValueExpr() == x) {
                ((SQLCaseExpr) parent).setValueExpr(valueableExpr);
            } else {
                ((SQLCaseExpr) parent).setElseExpr(valueableExpr);
            }
        } else if (parent instanceof SQLCaseExpr.Item) {
            if (((SQLCaseExpr.Item) parent).getConditionExpr() == x) {
                ((SQLCaseExpr.Item) parent).setConditionExpr(valueableExpr);
            } else {
                ((SQLCaseExpr.Item) parent).setValueExpr(valueableExpr);
            }
        } else if (parent instanceof SQLSelectItem) {
            ((SQLSelectItem) parent).setExpr(valueableExpr);
        } else if (parent instanceof SQLLimit) {
            if (((SQLLimit) parent).getRowCount() == x) {
                ((SQLLimit) parent).setRowCount(valueableExpr);
            } else {
                ((SQLLimit) parent).setOffset(valueableExpr);
            }
        } else if (parent instanceof SQLCastExpr) {
            ((SQLCastExpr) parent).setExpr(valueableExpr);
        } else if (parent instanceof SQLInsertStatement.ValuesClause) {
            List<SQLExpr> targetList = ((SQLInsertStatement.ValuesClause) parent).getValues();
            int replaceIndex = -1;
            for (int i = 0; i < targetList.size(); i++) {
                SQLExpr expr = targetList.get(i);
                if (!(expr instanceof SQLVariantRefExpr)) {
                    continue;
                }
                SQLVariantRefExpr variantRefExpr = (SQLVariantRefExpr) expr;
                if (variantRefExpr.getIndex() == x.getIndex()) {
                    replaceIndex = i;
                    break;
                }
            }
            targetList.set(replaceIndex, valueableExpr);
        } else if (parent instanceof SQLUpdateSetItem) {
            ((SQLUpdateSetItem) parent).setValue(valueableExpr);
        } else if (parent instanceof SQLBetweenExpr) {
            if (((SQLBetweenExpr) parent).getBeginExpr() == x) {
                ((SQLBetweenExpr) parent).setBeginExpr(valueableExpr);
            } else {
                ((SQLBetweenExpr) parent).setEndExpr(valueableExpr);
            }
        } else if (parent instanceof MySqlSelectQueryBlock) {
            if (((MySqlSelectQueryBlock) parent).getWhere() == x) {
                ((MySqlSelectQueryBlock) parent).setWhere(valueableExpr);
            }
        }
        return true;
    }

    @Override
    public boolean visit(final SQLIntervalExpr x) {
        SQLExpr valueExpr = x.getValue();
        if (valueExpr instanceof SQLVariantRefExpr) {
            List<SQLValuableExpr> valueableExprList = this.getValueableExprList((SQLVariantRefExpr) valueExpr);
            x.setValue(valueableExprList.get(0));
            return false;
        }
        return true;
    }

    private List<SQLValuableExpr> getValueableExprList(final SQLVariantRefExpr x) {
        List<SQLValuableExpr> valuableExprList = new ArrayList<>();

        try {
            BindVariable bindVariable = this.bindVariableMap.get(x.getName().replaceAll(":", ""));
            if (bindVariable == null) {
                bindVariable = this.bindVariableMap.get(String.valueOf(x.getIndex()));
            }
            if (Query.Type.TUPLE.equals(bindVariable.getType())) {
                List<VtValue> vtValueList = VtValue.newVtValueList(bindVariable);
                for (VtValue vtValue : vtValueList) {
                    SQLValuableExpr valueableExpr = this.getValueableExpr(vtValue);
                    valuableExprList.add(valueableExpr);
                }
            } else {
                VtValue vtValue = VtValue.newVtValue(bindVariable);
                SQLValuableExpr valueableExpr = this.getValueableExpr(vtValue);
                valuableExprList.add(valueableExpr);
            }
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return valuableExprList;
    }

    private SQLValuableExpr getValueableExpr(final VtValue vtValue) {
        SQLValuableExpr valuableExpr = null;

        switch (vtValue.getVtType()) {
            case VARBINARY:
            case VARCHAR:
            case TEXT:
            case TIME:
                valuableExpr = new SQLCharExpr(vtValue.toString());
                break;
            case BIT:
                valuableExpr = new SQLBooleanExpr(vtValue.toBoolean());
                break;
            case INT8:
            case INT16:
            case INT32:
                try {
                    valuableExpr = new SQLIntegerExpr(vtValue.toInt());
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
                break;
            case INT64:
                try {
                    valuableExpr = new SQLIntegerExpr(vtValue.toLong());
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
                break;
            case UINT64:
                valuableExpr = new SQLIntegerExpr(new BigInteger(vtValue.toString()));
                break;
            case DECIMAL:
            case FLOAT64:
            case FLOAT32:
                valuableExpr = new SQLNumberExpr(vtValue.toDecimal());
                break;
            case DATE:
                valuableExpr = new SQLDateExpr(vtValue.toString());
                break;
            case DATETIME:
            case TIMESTAMP:
                valuableExpr = new SQLTimestampExpr(vtValue.toString());
                break;
            case BLOB:
            case BINARY:
                valuableExpr = new SQLHexExpr(Hex.encodeHexString(vtValue.getVtValue()));
                break;
            case NULL_TYPE:
                valuableExpr = new SQLNullExpr();
                break;
            default:
                break;
        }
        return valuableExpr;
    }

    private SQLValuableExpr getValueableExpr(final SQLVariantRefExpr x) {
        BindVariable bindVariable;
        if (LimitEngine.LIMIT_VAR_REFINDEX == x.getIndex() || LimitEngine.LIMIT_VAR_NAME.equals(x.getName())) {
            bindVariable = this.bindVariableMap.get(x.getName().replaceAll(":", ""));
        } else if (InsertEngine.Generate.SEQ_VAR_REFINDEX == x.getIndex()) {
            bindVariable = this.bindVariableMap.get(x.getName().replaceAll(":", ""));
        } else if (DEFAULT_VAR_REFINDEX == x.getIndex()) {
            bindVariable = this.bindVariableMap.get(x.getName().replaceAll(":", ""));
        } else {
            bindVariable = this.bindVariableMap.get(String.valueOf(x.getIndex()));
        }
        boolean isSeq = x.getName().startsWith(InsertEngine.Generate.SEQ_VAR_NAME);

        SQLValuableExpr valuableExpr = null;
        try {
            VtValue bv = VtValue.newVtValue(bindVariable);
            Query.Type vtType = bv.getVtType();
            switch (vtType) {
                case VARBINARY:
                case VARCHAR:
                case TEXT:
                case TIME:
                    valuableExpr = new SQLCharExpr(bv.toString());
                    break;
                case BIT:
                    valuableExpr = new SQLBooleanExpr(bv.toBoolean());
                    break;
                case INT8:
                case INT16:
                case INT32:
                    try {
                        valuableExpr = new SQLIntegerExpr(bv.toInt());
                    } catch (SQLException e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                    break;
                case INT64:
                    try {
                        valuableExpr = new SQLIntegerExpr(bv.toLong());
                    } catch (SQLException e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                    break;
                case UINT64:
                    valuableExpr = new SQLIntegerExpr(new BigInteger(bv.toString()));
                    break;
                case DECIMAL:
                case FLOAT64:
                case FLOAT32:
                    valuableExpr = new SQLNumberExpr(bv.toDecimal());
                    break;
                case DATE:
                    valuableExpr = new SQLDateExpr(bv.toString());
                    break;
                case DATETIME:
                case TIMESTAMP:
                    valuableExpr = new SQLTimestampExpr(bv.toString());
                    break;
                case BLOB:
                case BINARY:
                    valuableExpr = new SQLHexExpr(Hex.encodeHexString(bv.getVtValue()));
                    break;
                case NULL_TYPE:
                    if (isSeq) {
                        BindVariable variable = this.bindVariableMap.get(x.getName());
                        VtValue value = VtValue.newVtValue(variable);
                        valuableExpr = new SQLIntegerExpr(value.toLong());

                    } else {
                        valuableExpr = new SQLNullExpr();
                    }
                    break;
                default:
                    break;
            }
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return valuableExpr;
    }
}
