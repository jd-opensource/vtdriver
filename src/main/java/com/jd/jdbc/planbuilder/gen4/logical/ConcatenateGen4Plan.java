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

import com.jd.jdbc.context.PlanningContext;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.gen4.ConcatenateGen4Engine;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import lombok.Getter;

public class ConcatenateGen4Plan extends LogicalPlanCommon implements LogicalPlan {
    @Getter
    private List<LogicalPlan> sources;

    // These column offsets do not need to be typed checked - they usually contain weight_string()
    // columns that are not going to be returned to the user
    @Getter
    private List<Integer> noNeedToTypeCheck;

    public ConcatenateGen4Plan(List<LogicalPlan> sources) {
        this.sources = sources;
        this.noNeedToTypeCheck = new ArrayList<>();
    }

    @Override
    public void wireupGen4(PlanningContext ctx) throws SQLException {
        for (LogicalPlan source : sources) {
            source.wireupGen4(ctx);
        }
    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() throws SQLException {
        List<PrimitiveEngine> engines = new ArrayList<>();
        for (LogicalPlan source : sources) {
            engines.add(source.getPrimitiveEngine());
        }
        return new ConcatenateGen4Engine(engines, new HashMap<>());
    }
}
