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

import com.google.protobuf.ByteString;
import com.jd.jdbc.srvtopo.BindVariable;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SqlTypes {

    /**
     * BvSchemaName is bind variable to be sent down to vttablet for schema name.
     */
    public static final String BV_SCHEMA_NAME = "__vtschemaname";

    public static final String BV_REPLACE_SCHEMA_NAME = "__replacevtschemaname";

    /**
     * ValueBindVariable converts a Value to a bind var.
     *
     * @param v
     * @return
     */
    public static BindVariable valueBindVariable(VtValue v) {
        if (v.isNull()) {
            return BindVariable.NULL_BIND_VARIABLE;
        }
        return new BindVariable(v.getVtValue(), v.getVtType());
    }

    public static BindVariable valueBindVariable(VtResultValue v) {
        if (v.isNull()) {
            return BindVariable.NULL_BIND_VARIABLE;
        }
        return new BindVariable(v.toBytes(), v.getVtType());
    }

    public static BindVariable valuesBindVariable(Query.Type type, List<VtValue> values) {
        List<Query.Value> valuesList = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            Query.Value value = Query.Value.newBuilder().setType(values.get(i).getVtType()).setValue(ByteString.copyFrom(values.get(i).getVtValue())).build();
            valuesList.add(value);
        }
        return new BindVariable(valuesList, type);
    }

    /**
     * Int64BindVariable converts an int64 to a bind var.
     *
     * @param v
     * @return
     * @throws Exception
     */
    public static BindVariable int64BindVariable(Long v) throws SQLException {
        return valueBindVariable(VtValue.newVtValue(Query.Type.INT64, String.valueOf(v).getBytes()));
    }

    /**
     * Float64BindVariable converts a float64 to a bind var.
     *
     * @param v
     * @return
     */
    public static BindVariable float64BindVariable(Float v) {
        return new BindVariable(String.valueOf(v).getBytes(), Query.Type.FLOAT64);
    }

    /**
     * BytesBindVariable converts a []byte to a bind var.
     *
     * @param v
     * @return
     */
    public static BindVariable bytesBindVariable(byte[] v) {
        return new BindVariable(v, Query.Type.VARBINARY);
    }

    /**
     * StringBindVariable converts a string to a bind var.
     *
     * @param v
     * @return
     */
    public static BindVariable stringBindVariable(String v) throws SQLException {
        return valueBindVariable(VtValue.newVtValue(Query.Type.VARBINARY, v.getBytes()));
    }

    /**
     * converts Value to a *querypb.Value.
     *
     * @param vtValue
     * @return
     */
    public static Query.Value vtValueToProto(VtResultValue vtValue) {
        return Query.Value.newBuilder().setType(vtValue.getVtType()).setValue(ByteString.copyFrom(vtValue.toBytes())).build();
    }

    public static Query.Value vtValueToProto(VtValue vtValue) {
        return Query.Value.newBuilder().setType(vtValue.getVtType()).setValue(ByteString.copyFrom(vtValue.toBytes())).build();
    }
}
