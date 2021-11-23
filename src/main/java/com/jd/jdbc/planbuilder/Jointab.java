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

import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import io.netty.util.internal.StringUtil;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * jointab manages procurement and naming of join
 * variables across primitives.
 */
@Setter
@Getter
public class Jointab {
    private Map<Column, String> refs;

    private Set<String> vars;

    private Integer varIndex = 0;

    /**
     * newJointab creates a new jointab for the current plan
     * being built. It also needs the current list of bind vars
     * used in the original query to make sure that the names
     * it generates don't collide with those already in use.
     *
     * @param bindVars
     * @return
     */
    public static Jointab newJointab(Set<String> bindVars) {
        Jointab jointab = new Jointab();
        jointab.setVars(bindVars);
        jointab.setRefs(new HashMap<>(16, 1));
        return jointab;
    }

    public String procure(Builder bldr, SQLName col, Integer to) throws SQLException {
        LookupResponse lookupResponse = this.lookup(col);
        Integer from = lookupResponse.getOrder();
        String joinVar = lookupResponse.getJoinVar();
        if (StringUtil.isNullOrEmpty(joinVar)) {
            String suffix = "";
            int i = 0;
            for (; ; ) {
                if (col instanceof SQLPropertyExpr
                    && ((SQLPropertyExpr) col).getOwner() != null) {
                    String ownerName = "";
                    if (((SQLPropertyExpr) col).getOwner() instanceof SQLIdentifierExpr) {
                        ownerName = ((SQLIdentifierExpr) ((SQLPropertyExpr) col).getOwner()).getName();
                    } else if (((SQLPropertyExpr) col).getOwner() instanceof SQLPropertyExpr) {
                        ownerName = ((SQLPropertyExpr) ((SQLPropertyExpr) col).getOwner()).getName();
                    }
                    joinVar = ownerName
                        + "_"
                        + Engine.compliantName(((SQLPropertyExpr) col).getName())
                        + suffix;
                } else {
                    joinVar = Engine.compliantName(col.getSimpleName()) + suffix;
                }
                if (!this.vars.contains(joinVar)) {
                    break;
                }
                i++;
                suffix = Integer.toString(i);
            }
            this.vars.add(joinVar);
            this.refs.put(col.getMetadata(), joinVar);
        }
        bldr.supplyVar(from, to, col, joinVar);
        return joinVar;
    }

    public GenerateSubqueryVarResponse generateSubqueryVars() throws SQLException {
        for (; ; ) {
            this.varIndex++;
            String suffix = Integer.toString(this.varIndex);
            String var1 = "__sq" + suffix;
            String var2 = "__sq_has_values" + suffix;
            if (this.containsAny(var1, var2)) {
                continue;
            }
            return new GenerateSubqueryVarResponse(var1, var2);
        }
    }

    private Boolean containsAny(String... names) {
        for (String name : names) {
            if (this.vars.contains(name)) {
                return true;
            }
            this.vars.add(name);
        }
        return false;
    }

    /**
     * Lookup returns the order of the route that supplies the column and
     * the join var name if one has already been assigned for it.
     *
     * @param col
     * @return
     */
    private LookupResponse lookup(SQLName col) {
        Column c = col.getMetadata();
        String joinVar = "";
        for (Map.Entry<Column, String> entry : this.refs.entrySet()) {
            if (entry.getKey() == c) {
                joinVar = entry.getValue();
                break;
            }
        }
        return new LookupResponse(c.origin().order(), joinVar);
    }

    @Getter
    @AllArgsConstructor
    public static class GenerateSubqueryVarResponse {
        private final String sq;

        private final String hasValues;
    }

    @Getter
    @AllArgsConstructor
    public static class LookupResponse {
        private final Integer order;

        private final String joinVar;
    }
}
