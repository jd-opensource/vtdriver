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

import io.vitess.proto.Query;
import java.sql.SQLException;
import org.junit.Assert;
import org.junit.Test;

public class VtValueTest {
    @Test
    public void testBoolean() throws SQLException {
        VtValue vtValue = VtValue.newVtValue(Query.Type.INT8, "1".getBytes());
        Assert.assertTrue(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.INT8, "-1".getBytes());
        Assert.assertTrue(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.INT8, "0".getBytes());
        Assert.assertFalse(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.UINT8, "1".getBytes());
        Assert.assertTrue(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.UINT8, "2".getBytes());
        Assert.assertTrue(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.UINT8, "0".getBytes());
        Assert.assertFalse(vtValue.toBoolean());

        vtValue = VtValue.newVtValue(Query.Type.INT64, "1".getBytes());
        Assert.assertTrue(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.INT64, "-1".getBytes());
        Assert.assertTrue(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.INT64, "0".getBytes());
        Assert.assertFalse(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.UINT64, "1".getBytes());
        Assert.assertTrue(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.UINT64, "2".getBytes());
        Assert.assertTrue(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.UINT64, "0".getBytes());
        Assert.assertFalse(vtValue.toBoolean());

        vtValue = VtValue.newVtValue(Query.Type.FLOAT32, "1".getBytes());
        Assert.assertTrue(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.FLOAT32, "-1".getBytes());
        Assert.assertTrue(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.FLOAT32, "0".getBytes());
        Assert.assertFalse(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.FLOAT64, "1".getBytes());
        Assert.assertTrue(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.FLOAT64, "-1".getBytes());
        Assert.assertTrue(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.FLOAT64, "0".getBytes());
        Assert.assertFalse(vtValue.toBoolean());

        vtValue = VtValue.newVtValue(Query.Type.DECIMAL, "1".getBytes());
        Assert.assertTrue(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.DECIMAL, "-1".getBytes());
        Assert.assertTrue(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.DECIMAL, "0".getBytes());
        Assert.assertFalse(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.DECIMAL, "1".getBytes());
        Assert.assertTrue(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.DECIMAL, "-1".getBytes());
        Assert.assertTrue(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.DECIMAL, "0".getBytes());
        Assert.assertFalse(vtValue.toBoolean());

        vtValue = VtValue.newVtValue(Query.Type.BIT, new byte[] {-1, -1, -1, -1, -1, -1, -1, -1});
        Assert.assertTrue(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.BIT, new byte[] {7, -8, 7, -8});
        Assert.assertTrue(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.BIT, new byte[] {0, 0, 0, 0});
        Assert.assertFalse(vtValue.toBoolean());

        vtValue = VtValue.newVtValue(Query.Type.VARBINARY, "y".getBytes());
        Assert.assertTrue(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.VARBINARY, "true".getBytes());
        Assert.assertTrue(vtValue.toBoolean());

        vtValue = VtValue.newVtValue(Query.Type.VARBINARY, "n".getBytes());
        Assert.assertFalse(vtValue.toBoolean());
        vtValue = VtValue.newVtValue(Query.Type.VARBINARY, "false".getBytes());
        Assert.assertFalse(vtValue.toBoolean());
    }
}
