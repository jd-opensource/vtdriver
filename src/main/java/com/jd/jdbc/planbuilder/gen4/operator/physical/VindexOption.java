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

import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.vindexes.Vindex;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

/**
 * VindexOption stores the information needed to know if we have all the information needed to use a vindex
 */
@Getter
@Setter
public class VindexOption {
    private boolean ready;

    private List<EvalEngine.Expr> values;

    /**
     * columns that we have seen so far. Used only for multi-column vindexes so that we can track how many columns part of the vindex we have seen
     */
    private Map<String, Object> colsSeen;

    private List<SQLExpr> valueExprs;

    private List<SQLExpr> predicates;

    private Engine.RouteOpcode opCode;

    private Vindex foundVindex;

    private Cost cost;
}