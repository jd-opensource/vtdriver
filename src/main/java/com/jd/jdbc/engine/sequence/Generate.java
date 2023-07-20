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

package com.jd.jdbc.engine.sequence;

import com.google.common.collect.Lists;
import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.key.Bytes;
import com.jd.jdbc.key.Destination;
import com.jd.jdbc.key.DestinationKeyspaceID;
import com.jd.jdbc.sqltypes.SqlTypes;
import com.jd.jdbc.sqltypes.VtPlanValue;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.srvtopo.ResolvedShard;
import com.jd.jdbc.srvtopo.Resolver;
import com.jd.jdbc.vindexes.VKeyspace;
import io.netty.util.internal.StringUtil;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Generate {
    private static final SequenceCache SEQUENCE_CACHE = new SequenceCache();

    public static final Integer SEQ_VAR_REFINDEX = -3;

    public static final String SEQ_VAR_NAME = ":__seq";

    private final VKeyspace keyspace;

    /**
     * Values are the supplied values for the column, which
     * will be stored as a list within the PlanValue. New
     * values will be generated based on how many were not
     * supplied (NULL).
     */
    private final VtPlanValue vtPlanValue;

    private final String sequenceTableName;

    private final String pinned;

    /**
     * processGenerate generates new values using a sequence if necessary.
     * If no value was generated, it returns 0. Values are generated only
     * for cases where none are supplied.
     *
     * @param vcursor
     * @param bindVariableMap
     * @return
     * @throws Exception
     */
    public static long processGenerate(Vcursor vcursor, Generate generate, Map<String, BindVariable> bindVariableMap) throws SQLException {
        long insertId = 0L;
        if (generate == null) {
            return insertId;
        }

        // Scan input values to compute the number of values to generate, and
        // keep track of where they should be filled.
        List<VtValue> resolved = generate.getVtPlanValue().resolveList(bindVariableMap);
        int count = 0;
        for (VtValue val : resolved) {
            if (shouldGenerate(val)) {
                count++;
            }
        }

        List<Long> sequences = null;
        if (count != 0) {
            // If generation is needed, generate the requested number of values (as one call).
            Destination dst;
            if (!StringUtil.isNullOrEmpty(generate.getPinned())) {
                dst = new DestinationKeyspaceID(Bytes.decodeToByteArray(generate.getPinned()));
            } else {
                throw new SQLException(generate.getSequenceTableName() + " should be a pinned table");
            }
            Resolver.ResolveDestinationResult resolveDestinationResult = vcursor.resolveDestinations(generate.getKeyspace().getName(), null, Lists.newArrayList(dst));
            List<ResolvedShard> rss = resolveDestinationResult.getResolvedShards();
            if (rss.size() != 1) {
                throw new SQLException("processGenerate len(rss)=" + rss.size());
            }

            sequences = SEQUENCE_CACHE.getSequences(vcursor, rss.get(0), generate.getKeyspace().getName(), generate.getSequenceTableName(), count);
            insertId = sequences.get(0);
        }

        // Fill the holes where no value was supplied.
        for (int i = 0, j = 0; i < resolved.size(); i++) {
            VtValue v = resolved.get(i);
            if (shouldGenerate(v)) {
                bindVariableMap.put(Generate.SEQ_VAR_NAME.substring(1) + i, SqlTypes.int64BindVariable(sequences.get(j++)));
            } else {
                bindVariableMap.put(Generate.SEQ_VAR_NAME.substring(1) + i, SqlTypes.valueBindVariable(v));
            }
        }

        return insertId;
    }

    /**
     * shouldGenerate determines if a sequence value should be generated for a given value
     *
     * @param vtValue
     * @return
     */
    private static boolean shouldGenerate(VtValue vtValue) {
        if (vtValue.isNull()) {
            return true;
        }

        // Unless the NO_AUTO_VALUE_ON_ZERO sql mode is active in mysql, it also
        // treats 0 as a value that should generate a new sequence.
        try {
            BigInteger n = EvalEngine.toUint64(vtValue);
            if (BigInteger.ZERO.equals(n)) {
                return true;
            }
        } catch (SQLException e) {
            return false;
        }
        return false;
    }
}
