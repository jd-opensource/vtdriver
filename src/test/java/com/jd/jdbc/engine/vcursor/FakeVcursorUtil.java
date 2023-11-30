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

package com.jd.jdbc.engine.vcursor;

import com.jd.jdbc.srvtopo.BindVariable;
import io.vitess.proto.Query;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FakeVcursorUtil {
    public static int testMaxMemoryRows = 100;

    public static boolean testIgnoreMaxMemoryRows = false;

    public static String printBindVars(Map<String, BindVariable> bindVariablesMap) {
        if (bindVariablesMap == null) {
            return "";
        }
        List<String> keys = new ArrayList<>();
        for (String key : bindVariablesMap.keySet()) {
            keys.add(key);
        }
        Collections.sort(keys);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) {
                sb.append(" ");
            }
            String k = keys.get(i);
            sb.append(k + ": " + printBindVariable(bindVariablesMap.get(k)));
        }

        return sb.toString();
    }

    public static String printBindVariable(BindVariable bv) {
        StringBuilder sb = new StringBuilder();
        if (bv.getValue() != null) {
            sb.append("type:" + bv.getType() + " value:\"" + new String(bv.getValue()) + "\"");
            return sb.toString();
        }
        if (bv.getValuesList() != null) {
            sb.append("type:" + bv.getType() + ", values:" + printValues(bv.getValuesList()));
            return sb.toString();
        }
        return "";
    }

    public static String printValues(List<Query.Value> ids) {
        if (ids == null) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[");
        for (Query.Value v : ids) {
            sb.append("type:");
            sb.append(v.getType());
            sb.append(" value:\"");
            String s = new String(v.getValue().toByteArray(), StandardCharsets.UTF_8);
            sb.append(s + "\"" + " ");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("]");
        return sb.toString();

    }

}
