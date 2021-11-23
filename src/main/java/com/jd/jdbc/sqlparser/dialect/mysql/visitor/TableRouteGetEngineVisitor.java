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
import com.jd.jdbc.sqlparser.ast.expr.SQLInListExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefExpr;
import com.jd.jdbc.sqltypes.VtPlanValue;
import java.util.List;
import java.util.Map;

public class TableRouteGetEngineVisitor extends SwitchTableVisitor {
    private final VtPlanValue planValue;

    public TableRouteGetEngineVisitor(final Map<String, String> switchTables, final VtPlanValue planValue) {
        super(switchTables);
        this.planValue = planValue;
    }

    public boolean visit(final SQLInListExpr x) {
        SQLExpr leftExpr = x.getExpr();
        leftExpr.accept(this);

        List<SQLExpr> valueList = x.getTargetList();
        if (valueList == null || valueList.size() != 1) {
            return false;
        }

        SQLExpr value = valueList.get(0);
        if (!(value instanceof SQLVariantRefExpr)) {
            return false;
        }

        if (this.planValue != null && ((SQLVariantRefExpr) value).getName().equals("::__vals")) {
            List<VtPlanValue> planValueList = this.planValue.getVtPlanValueList();
            for (int i = 0; i < planValueList.size(); i++) {
                SQLVariantRefExpr refExpr = new SQLVariantRefExpr("?");
                refExpr.setIndex(Integer.parseInt(planValueList.get(i).getKey()));
                if (i == 0) {
                    valueList.set(i, refExpr);
                } else {
                    valueList.add(refExpr);
                }
            }
        }

        return false;
    }
}
