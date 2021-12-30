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
import io.vitess.proto.Topodata;

public class RoleUtils {
    public static boolean notMaster(IContext ctx) {
        return getTabletType(ctx) != Topodata.TabletType.MASTER;
    }

    public static Topodata.TabletType getTabletType(IContext ctx) {
        return (Topodata.TabletType) ctx.getContextValue(Constant.DRIVER_PROPERTY_ROLE_KEY);
    }
}
