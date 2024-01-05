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

import com.jd.jdbc.planbuilder.semantics.TableSet;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import vschema.Vschema;

@Getter
@Setter
// VindexPlusPredicates is a struct used to store all the predicates that the vindex can be used to query
public class VindexPlusPredicates {
    private TableSet tableId;

    private Vschema.ColumnVindex colVindex;

    // during planning, we store the alternatives found for this route in this slice
    private List<VindexOption> options;

    public VindexPlusPredicates() {
        this.options = new ArrayList<>();
    }

    public VindexPlusPredicates(TableSet tableId, Vschema.ColumnVindex colVindex, List<VindexOption> options) {
        this.tableId = tableId;
        this.colVindex = colVindex;
        this.options = options;
    }

    public VindexOption bestOption() {
        VindexOption bestVindexOption = null;
        if (this.getOptions().size() > 0) {
            bestVindexOption = this.getOptions().get(0);

        }
        return bestVindexOption;
    }

    @Override
    public VindexPlusPredicates clone() {
        return new VindexPlusPredicates(this.tableId, this.colVindex, this.options);
    }

}
