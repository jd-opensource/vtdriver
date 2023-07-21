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

import io.vitess.proto.Query;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Column {
    private final String columnName;

    private final Query.Type type;

    private final LogicTable ltb;

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof Column)) {
            return false;
        }
        Column anotherCol = (Column) o;

        if (anotherCol.getColumnName() == null) {
            if (columnName != null) {
                return false;
            }
        } else if (!anotherCol.getColumnName().equalsIgnoreCase(columnName)) {
            return false;
        }

        if (anotherCol.getType() == null) {
            if (type != null) {
                return false;
            }
        } else if (!anotherCol.getType().equals(type)) {
            return false;
        }

        if (anotherCol.getLtb() == null) {
            return ltb == null;
        } else {
            return anotherCol.getLtb().equals(ltb);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, type, ltb);
    }
}
