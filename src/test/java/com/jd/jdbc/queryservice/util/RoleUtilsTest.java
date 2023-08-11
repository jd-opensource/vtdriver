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

package com.jd.jdbc.queryservice.util;

import com.jd.jdbc.queryservice.RoleType;
import io.vitess.proto.Topodata;
import java.sql.SQLException;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class RoleUtilsTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private RoleType rw = new RoleType(Topodata.TabletType.MASTER);

    private RoleType rr = new RoleType(Topodata.TabletType.REPLICA, Topodata.TabletType.RDONLY);

    private RoleType ro = new RoleType(Topodata.TabletType.RDONLY);

    private RoleType rrm = new RoleType(Topodata.TabletType.REPLICA, Topodata.TabletType.RDONLY, Topodata.TabletType.MASTER);

    @Test
    public void buildRoleTypeTest() throws SQLException {
        RoleType rrRoleType = RoleUtils.buildRoleType("rr");
        Assert.assertEquals(rr, rrRoleType);
        RoleType rwRoleType = RoleUtils.buildRoleType("rw");
        Assert.assertEquals(rw, rwRoleType);
        RoleType roRoleType = RoleUtils.buildRoleType("ro");
        Assert.assertEquals(ro, roRoleType);
        RoleType rrmRoleType = RoleUtils.buildRoleType("rrm");
        Assert.assertEquals(rrm, rrmRoleType);
    }

    @Test
    public void buildRoleTypeErrorTest() throws SQLException {
        String role = RandomStringUtils.random(3);
        thrown.expect(SQLException.class);
        thrown.expectMessage("'role=" + role + "' " + "error in jdbc url");
        RoleType roleType = RoleUtils.buildRoleType(role);
        Assert.fail();
    }
}