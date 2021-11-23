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

package com.jd.jdbc.sqltypes;

import io.netty.util.internal.StringUtil;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
public class VtPlanValue {
    private String key;

    private VtValue vtValue;

    private String listKey;

    private List<VtPlanValue> vtPlanValueList;

    public VtPlanValue() {
        this.vtPlanValueList = new ArrayList<>();
    }

    public VtPlanValue(String key) {
        this.key = key;
        this.vtValue = VtValue.NULL;
        this.vtPlanValueList = new ArrayList<>();
    }

    public VtPlanValue(VtValue vtValue) {
        this.vtValue = vtValue;
        this.vtPlanValueList = new ArrayList<>();
    }

    public VtPlanValue(List<VtPlanValue> vtPlanValueList) {
        this.vtValue = VtValue.NULL;
        this.vtPlanValueList = vtPlanValueList;
    }

    public Boolean isNull() {
        return StringUtil.isNullOrEmpty(this.key)
            && (this.vtValue == null || this.vtValue.isNull())
            && StringUtil.isNullOrEmpty(this.listKey)
            && (this.vtPlanValueList == null || this.vtPlanValueList.isEmpty());
    }

    /**
     * @param bindVariableMap
     * @return
     * @throws SQLException
     */
    public VtValue resolveValue(Map<String, Query.BindVariable> bindVariableMap) throws SQLException {
        if (!StringUtil.isNullOrEmpty(this.key)) {
            Query.BindVariable bv = this.lookupValue(bindVariableMap);
            return VtValue.newVtValue(bv.getType(), bv.getValue().toByteArray());
        } else if (this.vtValue != null && !this.vtValue.isNull()) {
            return this.vtValue;
        } else if (!StringUtil.isNullOrEmpty(this.listKey)) {
            // This code is unreachable because the parser does not allow
            // multi-value constructs where a single value is expected.
            throw new SQLException("a single value was supplied where a list was expected");
        } else if (this.vtPlanValueList != null && !this.vtPlanValueList.isEmpty()) {
            // This code is unreachable because the parser does not allow
            // multi-value constructs where a single value is expected.
            throw new SQLException("a single value was supplied where a list was expected");
        }
        return VtValue.NULL;
    }

    public Boolean isList() {
        return (this.listKey != null && !this.listKey.isEmpty()) || (this.vtPlanValueList != null && this.vtPlanValueList.size() > 0);
    }

    /**
     * @param bindVariableMap
     * @return
     * @throws SQLException
     */
    public List<VtValue> resolveList(Map<String, Query.BindVariable> bindVariableMap) throws SQLException {
        if (!StringUtil.isNullOrEmpty(this.listKey)) {
            Query.BindVariable bv = this.lookupList(bindVariableMap);
            List<VtValue> vtValueList = new ArrayList<>();
            for (Query.Value value : bv.getValuesList()) {
                vtValueList.add(VtValue.newVtValue(value.getType(), value.getValue().toByteArray()));
            }
            return vtValueList;
        } else if (this.vtPlanValueList != null && !this.vtPlanValueList.isEmpty()) {
            List<VtValue> vtValueList = new ArrayList<>();
            for (VtPlanValue vtPlanValue : this.vtPlanValueList) {
                VtValue vtValue = vtPlanValue.resolveValue(bindVariableMap);
                vtValueList.add(vtValue);
            }
            return vtValueList;
        }
        // This code is unreachable because the parser does not allow
        // single value constructs where multiple values are expected.
        throw new SQLException("a single value was supplied where a list was expected");
    }

    /**
     * @param bindVariableMap
     * @return
     * @throws SQLException
     */
    private Query.BindVariable lookupValue(Map<String, Query.BindVariable> bindVariableMap) throws SQLException {
        if (!bindVariableMap.containsKey(this.key)) {
            throw new SQLException("missing bind var " + this.key);
        }
        Query.BindVariable bv = bindVariableMap.get(this.key);
        if (Query.Type.TUPLE.equals(bv.getType())) {
            throw new SQLException("TUPLE was supplied for single value bind var " + this.listKey);
        }
        return bv;
    }

    /**
     * @param bindVariableMap
     * @return
     * @throws SQLException
     */
    private Query.BindVariable lookupList(Map<String, Query.BindVariable> bindVariableMap) throws SQLException {
        if (!bindVariableMap.containsKey(this.listKey)) {
            throw new SQLException("missing bind var " + this.listKey);
        }
        Query.BindVariable bv = bindVariableMap.get(this.listKey);
        if (!Query.Type.TUPLE.equals(bv.getType())) {
            throw new SQLException("single value was supplied for TUPLE bind var " + this.listKey);
        }
        return bv;
    }
}
