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

import com.jd.jdbc.sqlparser.SqlParser;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBooleanExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLCharExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLDateExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLHexExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntegerExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNullExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNumberExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLTimestampExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLValuableExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.srvtopo.BindVariable;
import io.vitess.proto.Query;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.codec.binary.Hex;


public class VtRestoreVisitor extends MySqlOutputVisitor {

    private final Map<String, BindVariable> bindVariableMap;

    private String charEncoding;

    @Setter
    private Map<String, String> switchTables = null;

    @Getter
    private SQLException exception;

    private VtRestoreVisitor(final Appendable appender, final Map<String, BindVariable> bindVariableMap) {
        super(appender, false);
        this.bindVariableMap = bindVariableMap;
        super.setPrettyFormat(false);
        super.setUppCase(false);
    }

    public VtRestoreVisitor(final Appendable appender, final Map<String, BindVariable> bindVariableMap, final String charEncoding) {
        this(appender, bindVariableMap);
        this.charEncoding = charEncoding;
    }

    private VtRestoreVisitor(final Appendable appender, final Map<String, BindVariable> bindVariableMap, final Map<String, String> switchTables) {
        this(appender, bindVariableMap);
        this.switchTables = switchTables;
    }

    public VtRestoreVisitor(final Appendable appender, final Map<String, BindVariable> bindVariableMap, final Map<String, String> switchTables, final String charEncoding) {
        this(appender, bindVariableMap, switchTables);
        this.charEncoding = charEncoding;
    }

    @Override
    public boolean visit(final SQLPropertyExpr x) {
        SQLExpr owner = x.getOwner();
        if (owner instanceof SQLIdentifierExpr) {
            String ownerName = ((SQLIdentifierExpr) owner).getName();
            String ownerNameLower = ownerName.toLowerCase();
            if (switchTables != null && switchTables.containsKey(ownerNameLower)) {
                String mapTableName = switchTables.get(ownerNameLower);
                print0(mapTableName);
            } else {
                print0(ownerName);
            }
        } else if (owner instanceof SQLPropertyExpr) {
            owner.accept(this);
        }
        print('.');
        String originalName = x.getName();

        if (SqlParser.MYSQL_KEYWORDS.getKeyword(originalName.toUpperCase()) != null) {
            print0("`" + originalName + "`");
        } else {
            print0(originalName);
        }

        return false;
    }

    @Override
    public boolean visit(final SQLExprTableSource x) {
        SQLExprTableSource cloneTableExpr = x.clone();
        String originTableName = cloneTableExpr.getName().getSimpleName();
        String ownerNameLower = originTableName.toLowerCase();
        if (switchTables != null && switchTables.containsKey(ownerNameLower)) {
            if (cloneTableExpr.getExpr() instanceof SQLPropertyExpr) {
                ((SQLPropertyExpr) cloneTableExpr.getExpr()).setName(switchTables.get(ownerNameLower));
            } else {
                cloneTableExpr.setExpr(switchTables.get(originTableName));
            }
        }

        printTableSourceExpr(cloneTableExpr.getExpr());

        String alias = x.getAlias();
        if (alias != null) {
            print(' ');
            print0("as " + alias);
        }

        for (int i = 0; i < x.getHintsSize(); ++i) {
            print(' ');
            x.getHints().get(i).accept(this);
        }

        if (x.getPartitionSize() > 0) {
            print0(ucase ? " PARTITION (" : " partition (");
            printlnAndAccept(x.getPartitions(), ", ");
            print(')');
        }

        return false;
    }

    @Override
    public boolean visit(final SQLVariantRefExpr x) {
        List<SQLValuableExpr> valueableExprList = this.getValueableExprList(x);
        if (valueableExprList.size() != 1) {
            for (int i = 0; i < valueableExprList.size(); i++) {
                SQLValuableExpr valuableExpr = valueableExprList.get(i);
                super.printParameter(valuableExpr.getValue());
                if (i + 1 != valueableExprList.size()) {
                    super.print0(",");
                }
            }
            return false;
        }
        SQLValuableExpr valueableExpr = valueableExprList.get(0);
        if (valueableExpr instanceof SQLNullExpr) {
            super.printParameter(null);
        } else {
            super.printParameter(valueableExpr.getValue());
        }
        return true;
    }

    @Override
    public boolean visit(final SQLIdentifierExpr x) {
        String originalName = x.getName();

        if (SqlParser.MYSQL_KEYWORDS.getKeyword(originalName.toUpperCase()) != null) {
            print0("`" + originalName + "`");
        } else {
            print0(originalName);
        }
        return false;
    }

    private List<SQLValuableExpr> getValueableExprList(final SQLVariantRefExpr x) {
        List<SQLValuableExpr> valuableExprList = new ArrayList<>();

        try {
            String name = StringUtils.replaceEach(x.getName(), new String[] {":"}, new String[] {""});
            BindVariable bindVariable = this.bindVariableMap.get(name);
            if (bindVariable == null) {
                bindVariable = this.bindVariableMap.get(String.valueOf(x.getIndex()));
            }
            if (Query.Type.TUPLE.equals(bindVariable.getType())) {
                List<VtValue> vtValueList = VtValue.newVtValueList(bindVariable);
                for (VtValue vtValue : vtValueList) {
                    if (vtValue.getVtType() == Query.Type.VARBINARY) {
                        vtValue.setCharEncoding(this.charEncoding);
                    }
                    SQLValuableExpr valueableExpr = this.getValueableExpr(vtValue);
                    valuableExprList.add(valueableExpr);
                }
            } else {
                VtValue vtValue = VtValue.newVtValue(bindVariable);
                if (vtValue.getVtType() == Query.Type.VARBINARY) {
                    vtValue.setCharEncoding(this.charEncoding);
                }
                SQLValuableExpr valueableExpr = this.getValueableExpr(vtValue);
                valuableExprList.add(valueableExpr);
            }
        } catch (SQLException e) {
            this.exception = e;
        }
        return valuableExprList;
    }

    private SQLValuableExpr getValueableExpr(final VtValue vtValue) throws SQLException {
        SQLValuableExpr valuableExpr = null;

        switch (vtValue.getVtType()) {
            case CHAR:
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
            case UINT8:
            case INT16:
            case UINT16:
            case INT24:
            case UINT24:
            case INT32:
                valuableExpr = new SQLIntegerExpr(vtValue.toInt());
                break;
            case UINT32:
            case INT64:
                valuableExpr = new SQLIntegerExpr(vtValue.toLong());
                break;
            case UINT64:
                valuableExpr = new SQLIntegerExpr(new BigInteger(vtValue.toString()));
                break;
            case DECIMAL:
            case FLOAT32:
            case FLOAT64:
                valuableExpr = new SQLNumberExpr(vtValue.toDecimal());
                break;
            case DATE:
            case YEAR:
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
                throw new SQLException("unknown data type:" +  vtValue.getVtType());
        }
        return valuableExpr;
    }
}
