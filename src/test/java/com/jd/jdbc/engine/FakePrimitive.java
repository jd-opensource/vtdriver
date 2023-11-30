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

package com.jd.jdbc.engine;

import com.jd.jdbc.IExecute;
import com.jd.jdbc.context.IContext;
import static com.jd.jdbc.engine.vcursor.FakeVcursorUtil.printBindVars;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.srvtopo.BindVariable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.junit.Assert;

/**
 * fakePrimitive fakes a primitive. For every call, it sends the
 * next result from the results. If the next result is nil, it
 * returns sendErr. For streaming calls, it sends the field info
 * first and two rows at a time till all rows are sent.
 */
@AllArgsConstructor
public class FakePrimitive implements PrimitiveEngine {

    private List<VtResultSet> resultSet;

    private int curResult;

    // sendErr is sent at the end of the stream if it's set.
    private SQLException sendErr;

    private List<String> log = new ArrayList<>();

    private boolean allResultsInOneCall;

    public FakePrimitive(List<VtResultSet> resultSet) {
        this.resultSet = resultSet;
        log = new ArrayList<>();
    }

    public FakePrimitive(List<VtResultSet> resultSet, SQLException err) {
        this.resultSet = resultSet;
        this.sendErr = err;
        log = new ArrayList<>();
    }

    public FakePrimitive(SQLException sendErr) {
        this.sendErr = sendErr;
    }

    @Override
    public String getKeyspaceName() {
        return "fakeKs";
    }

    @Override
    public String getTableName() {
        return "fakeTable";
    }

    @Override
    public IExecute.ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        log.add(String.format("Execute %s %s", printBindVars(bindVariableMap), wantFields));
        if (this.resultSet == null) {
            if (this.sendErr != null) {
                throw this.sendErr;
            }
            return null;
        }
        VtResultSet r = resultSet.get(curResult);
        this.curResult++;
        if (r == null) {
            if (this.sendErr != null) {
                throw this.sendErr;
            }
            return null;
        }
        return new IExecute.ExecuteMultiShardResponse(r);

    }

    @Override
    public VtResultSet getFields(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        log.add(String.format("GetFields %s", printBindVars(bindVariableMap)));
        return (VtResultSet) this.execute(null, vcursor, bindVariableMap, true).getVtRowList();
    }

    @Override
    public Boolean needsTransaction() {
        return false;
    }

    public void rewind() {
        this.curResult = 0;
        this.log = new ArrayList<>();
    }

    public void expectLog(List<String> want) {
        if (want.size() != log.size()) {
            Assert.fail("wants size " + want.size() + " actual size" + log.size());
        }
        for (int i = 0; i < want.size(); i++) {
            Assert.assertEquals(want.get(i), log.get(i));
        }
    }
}
