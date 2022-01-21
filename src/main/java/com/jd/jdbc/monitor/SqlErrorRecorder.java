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

package com.jd.jdbc.monitor;

import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.srvtopo.BindVariable;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SqlErrorRecorder {
    private String keyspace;

    private String errorMessage;

    private String errorClassName;

    private SQLStatement sqlStatement;

    private LocalDateTime errorTime;

    private Map<String, BindVariable> bindVariableMap;

    private String sql;

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SqlErrorRecorder recorder = (SqlErrorRecorder) o;
        return Objects.equals(keyspace, recorder.keyspace)
            && Objects.equals(errorMessage, recorder.errorMessage)
            && Objects.equals(errorClassName, recorder.errorClassName)
            && Objects.equals(sqlStatement, recorder.sqlStatement);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyspace, errorMessage, errorClassName, sqlStatement);
    }
}
