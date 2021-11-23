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

package com.jd.jdbc.topo;

import com.jd.jdbc.topo.topoproto.TopoProto;
import io.vitess.proto.Topodata;

public class TopoTabletInfo implements TopoConnection.Version {
    private TopoConnection.Version version;

    private Topodata.Tablet tablet;

    public TopoTabletInfo(TopoConnection.Version version, Topodata.Tablet tablet) {
        this.version = version;
        this.tablet = tablet;
    }

    /**
     * @return
     */
    @Override
    public String string() {
        return String.format("Tablet{%s}", TopoProto.tabletAliasString(this.tablet.getAlias()));
    }

    public TopoConnection.Version getVersion() {
        return version;
    }

    public void setVersion(TopoConnection.Version version) {
        this.version = version;
    }

    public Topodata.Tablet getTablet() {
        return tablet;
    }

    public void setTablet(Topodata.Tablet tablet) {
        this.tablet = tablet;
    }
}
