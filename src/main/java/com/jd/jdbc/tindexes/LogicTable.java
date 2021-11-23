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

import com.google.protobuf.ByteString;
import com.jd.jdbc.sqltypes.VtValue;
import io.vitess.proto.Query;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class LogicTable {
    private String logicTable;

    private List<ActualTable> actualTableList;

    private Column tindexCol;

    private TableIndex tableIndex;

    public LogicTable() {
    }

    public ActualTable map(final VtValue value) {
        if (value.getVtType() == Query.Type.NULL_TYPE) {
            return null;
        }
        int tablesNum = this.getActualTableList().size();
        int index = tableIndex.getIndex(ByteString.copyFrom(value.getVtValue()), tablesNum);
        return this.getActualTableList().get(index);
    }

    public ActualTable getFirstActualTable() {
        if (this.actualTableList == null || this.actualTableList.isEmpty()) {
            return null;
        }
        return this.actualTableList.get(0);
    }

    public String getFirstActualTableName() {
        if (this.actualTableList == null || this.actualTableList.isEmpty()) {
            return null;
        }
        ActualTable actualTable = this.actualTableList.get(0);
        if (actualTable == null) {
            return null;
        }
        return actualTable.getActualTableName();
    }
}
