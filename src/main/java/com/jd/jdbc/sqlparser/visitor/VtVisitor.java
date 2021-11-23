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

package com.jd.jdbc.sqlparser.visitor;

import com.google.common.collect.Sets;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLInListExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLLiteralExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLDeleteStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLInsertStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLUpdateStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqltypes.VtValue;
import io.vitess.proto.Query;
import lombok.Getter;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.jd.jdbc.sqltypes.SqlTypes.valueBindVariable;

@Getter
public class VtVisitor extends SQLASTVisitorAdapter {
    private static final Log log = LogFactory.getLog(VtVisitor.class);
    public static final String BIND_VAR_PREFIX = "__vtg";
    public static final String BIND_VAR_PREFIX_WITH_COLON = ":__vtg";

    private final String vIndex;

    private List<SQLExpr> routeValues;

    private IInsertParser insertParser;

    public VtVisitor(String vIndex) {
        this.vIndex = vIndex;
    }

    @Override
    public boolean visit(SQLUpdateStatement x) {
        try {
            routeValues = new BaseParser(x.getWhere()).parse();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        //remove schema
        ((SQLExprTableSource) x.getTableSource()).setSchema(null);
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLDeleteStatement x) {
        try {
            routeValues = new BaseParser(x.getWhere()).parse();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        //remove schema
        ((SQLExprTableSource) x.getTableSource()).setSchema(null);
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLSelectStatement x) {
        try {
            routeValues = new BaseParser(((MySqlSelectQueryBlock) x.getSelect().getQuery()).getWhere()).parse();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        //remove schema
        ((SQLExprTableSource) ((MySqlSelectQueryBlock) x.getSelect().getQuery()).getFrom()).setSchema(null);
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLInsertStatement x) {
        //remove schema
        x.getTableSource().setSchema(null);
        insertParser = (IInsertParser) new BaseParser(x).getParser(x);
        try {
            routeValues = insertParser.parse();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        return super.visit(x);
    }

    private interface IParser {
        List<SQLExpr> parse() throws SQLException;
    }

    public interface IInsertParser extends IParser {
        String getPrefix();

        String[] getMid();

        String getSuffix();

        Map<String, Query.BindVariable> getBindVars();
    }

    class BaseParser implements IParser {
        protected SQLObject astNode;

        public BaseParser(SQLObject astNode) {
            this.astNode = astNode;
        }

        @Override
        public List<SQLExpr> parse() throws SQLException {
            return getParser(astNode).parse();
        }

        public IParser getParser(SQLObject astNode) {
            if (astNode == null) {
                return new DefaultParser();
            }

            if (astNode instanceof SQLBinaryOpExpr) {
                if (((SQLBinaryOpExpr) astNode).getOperator().getName().equals("AND")) {
                    return new AndParser(astNode);
                }
                if (((SQLBinaryOpExpr) astNode).getOperator().getName().equals("OR")) {
                    return new OrParser(astNode);
                }
                if (((SQLBinaryOpExpr) astNode).getOperator().getName().equals("=")) {
                    return new EqualParser(astNode);
                }

                return new DefaultParser();
            }

            if (astNode instanceof SQLInListExpr) {
                return new InListParser(astNode);
            }

            if (astNode instanceof SQLInsertStatement) {
                return new InsertParser(astNode);
            }

            return new DefaultParser();
        }

        public Set<SQLExpr> intersection(Set<SQLExpr> x, Set<SQLExpr> y) {
            if (x == null) {
                return y;
            }

            if (y == null) {
                return x;
            }

            Set<SQLExpr> intersection = new HashSet<SQLExpr>(x);
            intersection.retainAll(y);

            return intersection;
        }

        public Set<SQLExpr> union(Set<SQLExpr> x, Set<SQLExpr> y) {
            if (x == null) {
                return null;
            }

            if (y == null) {
                return null;
            }

            Set<SQLExpr> unionSet = new HashSet<SQLExpr>(x);
            unionSet.addAll(y);

            return unionSet;
        }
    }

    private class DefaultParser extends BaseParser {
        public DefaultParser() {
            super(null);
        }

        @Override
        public List<SQLExpr> parse() {
            return null;
        }
    }

    private class InsertParser extends BaseParser implements IInsertParser {

        private String prefix;

        private String suffix;

        private String[] mid;

        private Map<String, Query.BindVariable> bindVars;

        public InsertParser(SQLObject astNode) {
            super(astNode);
        }

        @Override
        public List<SQLExpr> parse() throws SQLException {
            if (astNode instanceof SQLInsertStatement) {
                SQLInsertStatement x = (SQLInsertStatement) astNode;
                normalize(x);

                int vIdx = -1;
                for (int i = 0; i < x.getColumns().size(); i++) {
                    String colName = ((SQLIdentifierExpr) x.getColumns().get(i)).getName();
                    if (colName.equals(vIndex) || colName.equals("`" + vIndex + "`")) {
                        vIdx = i;
                        break;
                    }
                }
                if (vIdx == -1) {
                    return null;
                }

                int finalVIdx = vIdx;
                List<SQLExpr> routeValues = new ArrayList<>();
                x.getValuesList().forEach(row -> {
                    routeValues.add(row.getValues().get(finalVIdx));
                });

                return routeValues;
            }
            return null;
        }

        @Override
        public String getPrefix() {
            return prefix;
        }

        @Override
        public String[] getMid() {
            return mid;
        }

        @Override
        public String getSuffix() {
            return suffix;
        }

        @Override
        public Map<String, Query.BindVariable> getBindVars() {
            return bindVars;
        }

        private void normalize(SQLInsertStatement x) throws SQLException {
            StringBuffer sb = new StringBuffer();
            sb.append("insert into ").append(x.getTableName().getSimpleName()).append(" ").append(x.columnsToString()).append(" values ");
            prefix = sb.toString();
            suffix = "";
            bindVars = new HashMap<>();

            mid = new String[x.getValuesList().size()];
            for (int row = 0; row < x.getValuesList().size(); row++) {
                StringBuffer sbs = new StringBuffer();
                sbs.append("(");
                String pad = "";
                for (int col = 0; col < x.getValuesList().get(row).getValues().size(); col++) {
                    SQLExpr val = x.getValuesList().get(row).getValues().get(col);
                    VtValue vtValue = VtValue.newVtValue(val);
                    if (vtValue.isNull()) {
                        sbs.append(pad).append(val);
                        pad = ",";
                    } else {
                        String bindVarS = BIND_VAR_PREFIX + "_" + row + "_" + col;
                        sbs.append(pad).append(":").append(bindVarS);
                        pad = ",";
                        bindVars.put(bindVarS, valueBindVariable(VtValue.newVtValue(val)));
                    }
                }

                sbs.append(")");
                mid[row] = sbs.toString();
            }
        }
    }

    private class AndParser extends BaseParser {

        public AndParser(SQLObject astNode) {
            super(astNode);
        }

        @Override
        public List<SQLExpr> parse() {
            if (astNode instanceof SQLBinaryOpExpr) {
                SQLExpr astLeft = ((SQLBinaryOpExpr) astNode).getLeft();
                List<SQLExpr> left = null;
                try {
                    left = getParser(astLeft).parse();
                } catch (SQLException e) {
                    log.error(e.getMessage(), e);
                }

                SQLExpr astRight = ((SQLBinaryOpExpr) astNode).getRight();
                List<SQLExpr> right = null;
                try {
                    right = getParser(astRight).parse();
                } catch (SQLException e) {
                    log.error(e.getMessage(), e);
                }

                Set<SQLExpr> interset = intersection(null == left ? null : Sets.newHashSet(left),
                        null == right ? null : Sets.newHashSet(right));
                if (null == interset) {
                    return null;
                }
                return new ArrayList<>(interset);
            }
            return null;
        }
    }

    private class OrParser extends BaseParser {

        public OrParser(SQLObject astNode) {
            super(astNode);
        }

        @Override
        public List<SQLExpr> parse() {
            if (astNode instanceof SQLBinaryOpExpr) {
                SQLExpr astLeft = ((SQLBinaryOpExpr) astNode).getLeft();
                List<SQLExpr> left = null;
                try {
                    left = getParser(astLeft).parse();
                } catch (SQLException e) {
                    log.error(e.getMessage(), e);
                }
                if (null == left) {
                    return null;
                }

                SQLExpr astRight = ((SQLBinaryOpExpr) astNode).getRight();
                List<SQLExpr> right = null;
                try {
                    right = getParser(astRight).parse();
                } catch (SQLException e) {
                    log.error(e.getMessage(), e);
                }
                if (null == right) {
                    return null;
                }

                Set<SQLExpr> unset = union(Sets.newHashSet(left), Sets.newHashSet(right));
                if (null == unset) {
                    return null;
                }
                return new ArrayList<>(unset);
            }
            return null;
        }
    }

    private class InListParser extends BaseParser implements IParser {

        public InListParser(SQLObject astNode) {
            super(astNode);
        }

        @Override
        public List<SQLExpr> parse() {
            if (astNode instanceof SQLInListExpr) {
                if (((SQLInListExpr) astNode).getExpr().toString().equals(vIndex)) {
                    return ((SQLInListExpr) astNode).getTargetList();
                }
            }
            return null;
        }
    }

    private class EqualParser extends BaseParser implements IParser {

        public EqualParser(SQLObject astNode) {
            super(astNode);
        }

        @Override
        public List<SQLExpr> parse() {
            if (astNode instanceof SQLBinaryOpExpr) {
                SQLExpr left = ((SQLBinaryOpExpr) astNode).getLeft();
                SQLExpr right = ((SQLBinaryOpExpr) astNode).getRight();

                if (left.toString().equals(vIndex)) {
                    if (right instanceof SQLLiteralExpr || right instanceof SQLVariantRefExpr) {
                        return new ArrayList<>(Collections.singletonList(right));
                    }
                }
                if (right.toString().equals(vIndex)) {
                    if (left instanceof SQLLiteralExpr || left instanceof SQLVariantRefExpr) {
                        return new ArrayList<>(Collections.singletonList(left));
                    }
                }
            }
            return null;
        }
    }
}
