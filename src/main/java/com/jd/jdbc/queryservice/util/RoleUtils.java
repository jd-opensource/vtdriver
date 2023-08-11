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

package com.jd.jdbc.queryservice.util;

import com.jd.jdbc.common.Constant;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.queryservice.RoleType;
import io.vitess.proto.Topodata;
import java.sql.SQLException;

public class RoleUtils {

    public static boolean notMaster(IContext ctx) {
        return getTabletType(ctx) != Topodata.TabletType.MASTER;
    }

    public static Topodata.TabletType getTabletType(IContext ctx) {
        RoleType roleType = getRoleType(ctx);
        return roleType.getTargetTabletType();
    }

    public static RoleType getRoleType(IContext ctx) {
        return (RoleType) ctx.getContextValue(Constant.DRIVER_PROPERTY_ROLE_KEY);
    }

    public static RoleType buildRoleType(String role) throws SQLException {
        switch (role.toLowerCase()) {
            case Constant.DRIVER_PROPERTY_ROLE_RW:
                return new RoleType(Topodata.TabletType.MASTER);
            case Constant.DRIVER_PROPERTY_ROLE_RR:
                return new RoleType(Topodata.TabletType.REPLICA, Topodata.TabletType.RDONLY);
            case Constant.DRIVER_PROPERTY_ROLE_RO:
                return new RoleType(Topodata.TabletType.RDONLY);
            case Constant.DRIVER_PROPERTY_ROLE_RRM:
                return new RoleType(Topodata.TabletType.REPLICA, Topodata.TabletType.RDONLY, Topodata.TabletType.MASTER);
            default:
                throw new SQLException("'role=" + role + "' " + "error in jdbc url");
        }
    }
}
