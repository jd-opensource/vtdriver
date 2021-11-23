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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

/**
 * ResultsBuilder is a superset of builderCommon. It also handles
 * resultsColumn functionality.
 */
@Getter
@Setter
public class ResultsBuilder extends BuilderImpl {

    private List<ResultColumn> resultColumnList;

    private Map<ResultColumn, Integer> weightStrings;

    private Truncater truncater;

    public ResultsBuilder(Builder input, Truncater truncater) {
        super(input);
        this.resultColumnList = new ArrayList<>(input.resultColumns());
        this.weightStrings = new HashMap<>();
        this.truncater = truncater;
    }

    @Override
    public List<ResultColumn> resultColumns() {
        return this.resultColumnList;
    }

    @Override
    public Integer supplyWeightString(Integer colNumber) throws SQLException {
        ResultColumn rc = this.resultColumnList.get(colNumber);
        if (weightStrings.containsKey(rc)) {
            return weightStrings.get(rc);
        }
        Integer weightColNumber = this.getBldr().supplyWeightString(colNumber);
        this.getWeightStrings().put(rc, weightColNumber);
        if (weightColNumber < this.resultColumnList.size()) {
            return weightColNumber;
        }
        // Add result columns from input until weightcolNumber is reached.
        while (weightColNumber >= this.resultColumnList.size()) {
            this.resultColumnList.add(this.getBldr().resultColumns().get(this.resultColumnList.size()));
        }
        this.truncater.setTruncateColumnCount(this.resultColumnList.size());
        return weightColNumber;
    }
}
