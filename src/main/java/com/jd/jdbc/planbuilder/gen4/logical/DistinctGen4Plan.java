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

package com.jd.jdbc.planbuilder.gen4.logical;

import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.gen4.CheckCol;
import com.jd.jdbc.engine.gen4.DistinctGen4Engine;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

public class DistinctGen4Plan extends LogicalPlanCommon implements LogicalPlan {
    private List<CheckCol> checkCols;

    private boolean needToTruncate;

    public DistinctGen4Plan(LogicalPlan source, List<CheckCol> checkCols, boolean needToTruncate) {
        this.checkCols = checkCols;
        this.needToTruncate = needToTruncate;
        this.setInput(source);
    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() throws SQLException {
        if (this.checkCols == null) {
            // If we are missing the checkCols information, we are on the V3 planner and should produce a V3 Distinct
            throw new SQLFeatureNotSupportedException();
        }
        boolean truncate = false;
        if (this.needToTruncate) {
            for (CheckCol checkCol : this.checkCols) {
                if (checkCol.getWsCol() != null) {
                    truncate = true;
                    break;
                }
            }
        }
        return new DistinctGen4Engine(this.getInput().getPrimitiveEngine(), this.checkCols, truncate);
    }
}
