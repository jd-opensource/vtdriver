/*
Copyright 2021 JD Project Authors.

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

package com.jd.jdbc.tindexes;

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ActualTable implements Comparable<ActualTable> {

    private String actualTableName;

    private LogicTable logicTable;

    private int index;

    public ActualTable() {
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof ActualTable)) {
            return false;
        }
        ActualTable anotherTable = (ActualTable) o;
        if (anotherTable.getActualTableName() == null) {
            if (actualTableName != null) {
                return false;
            }
        } else if (!anotherTable.getActualTableName().equalsIgnoreCase(actualTableName)) {
            return false;
        }
        if (anotherTable.getLogicTable() == null) {
            return logicTable == null;
        } else {
            return anotherTable.getLogicTable().equals(logicTable);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(actualTableName, logicTable);
    }

    @Override
    public int compareTo(ActualTable o) {
        if (o == null) {
            return 1;
        }
        return Integer.compare(index, o.getIndex());
    }
}
