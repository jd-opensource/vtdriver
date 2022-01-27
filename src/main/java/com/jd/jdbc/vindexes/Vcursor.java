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

package com.jd.jdbc.vindexes;

import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.srvtopo.BindVariable;
import io.vitess.proto.Vtgate;
import java.util.List;
import java.util.Map;

public interface Vcursor {

    /**
     * @param method
     * @param query
     * @param bindVariables
     * @param rollbackOnError
     * @param commitOrder
     * @return
     */
    ExecuteResponse execute(String method, String query, Map<String, BindVariable> bindVariables, Boolean rollbackOnError, Vtgate.CommitOrder commitOrder);

    /**
     * @param keyspace
     * @param ksid
     * @param query
     * @param bindVariables
     * @param rollbackOnError
     * @param autocommit
     * @return
     */
    ExecuteResponse executeKeyspaceId(String keyspace, List<Byte> ksid, String query, Map<String, BindVariable> bindVariables, Boolean rollbackOnError, Boolean autocommit);

    class ExecuteResponse {
        VtResultSet resultSet;

        Exception exception;
    }
}
