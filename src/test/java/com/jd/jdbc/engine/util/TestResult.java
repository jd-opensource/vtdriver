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

package com.jd.jdbc.engine.util;

import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtType;
import static com.jd.jdbc.vitess.resultset.ResultSetUtil.convertValue;
import io.vitess.proto.Query;
import java.util.ArrayList;
import java.util.List;

public class TestResult {

    /**
     * Functions in this file should only be used for testing.
     * This is an experiment to see if test code bloat can be
     * reduced and readability improved.
     * <p>
     * MakeTestFields builds a []*querypb.Field for testing.
     * fields := sqltypes.MakeTestFields(
     * "a|b",
     * "int64|varchar",
     * )
     * The field types are as defined in querypb and are case
     * insensitive. Column delimiters must be used only to sepearate
     * strings and not at the beginning or the end.
     */

    public static Query.Field[] makeTestFields(String n, String t) {
        String[] names = splitStr(n);
        String[] types = splitStr(t);
        if (names.length != types.length) {
            throw new RuntimeException();
        }
        Query.Field[] res = new Query.Field[names.length];
        for (int i = 0; i < names.length; i++) {
            res[i] = Query.Field.newBuilder().setName(names[i]).setType(Query.Type.valueOf(types[i].toUpperCase())).build();
        }
        return res;
    }

    /**
     * MakeTestResult builds a *sqltypes.Result object for testing.
     * result := sqltypes.MakeTestResult(
     * fields,
     * " 1|a",
     * "10|abcd",
     * )
     * The field type values are set as the types for the rows built.
     * Spaces are trimmed from row values. "null" is treated as NULL.
     **/
    public static VtResultSet makeTestResult(Query.Field[] fields, String... rows) {
        VtResultSet res = new VtResultSet();
        res.setFields(fields);
        if (rows.length <= 0) {
            return res;
        }
        List<List<VtResultValue>> resRows = new ArrayList<>(rows.length);

        for (int i = 0; i < rows.length; i++) {
            List<VtResultValue> resRow = new ArrayList<>(fields.length);
            String[] row = splitStr(rows[i]);
            for (int j = 0; j < row.length; j++) {
                VtResultValue item;
                if (row[j].equals("null")) {
                    item = new VtResultValue(null, Query.Type.NULL_TYPE);
                } else {
                    Class<?> javaClass = VtType.DataTypeConverter.fromTypeString(fields[j].getType().toString()).getJavaClass();
                    Object o = convertValue(row[j], javaClass);
                    item = new VtResultValue(o, fields[j].getType());
                }
                resRow.add(item);
            }
            resRows.add(new ArrayList<>(resRow));
        }
        res.setRows(resRows);
        return res;
    }

    public static String[] splitStr(String str) {
        return str.split("\\|");
    }

}
