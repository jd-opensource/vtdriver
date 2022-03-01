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

package com.jd.jdbc.topo.topoproto;

import com.jd.jdbc.topo.TopoException;
import io.vitess.proto.Topodata;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class TopoProto {

    private static final int EXPECT_NAME_PARTS_LENGTH = 2;

    private static final String TEN_ZERO = "0000000000";

    private static final Map<Integer, String> tabletTypeLowerName;

    private static final Map<Topodata.TabletAlias, String> tabletAliasStringMap = new ConcurrentHashMap<>(16);

    private static final Map<Topodata.Tablet, String> poolNameMap = new ConcurrentHashMap<>(16);

    static {
        tabletTypeLowerName = new HashMap<>(Topodata.TabletType.values().length + 1, 1);
        for (int i = 0; i < Topodata.TabletType.values().length; i++) {
            Topodata.TabletType tabletType = Topodata.TabletType.forNumber(i);
            String name = "";
            if (tabletType != null) {
                name = tabletType.name();
            }
            tabletTypeLowerName.put(i, name.toLowerCase());
        }
    }

    /**
     * @param left
     * @param right
     * @return
     */
    public static Boolean tabletAliasEqual(Topodata.TabletAlias left, Topodata.TabletAlias right) {
        return left.getCell().equals(right.getCell()) && left.getUid() == right.getUid();
    }

    /**
     * @param tabletAlias
     * @return
     */
    public static String tabletAliasString(Topodata.TabletAlias tabletAlias) {
        if (tabletAlias == null) {
            return "null";
        }
        if (tabletAliasStringMap.containsKey(tabletAlias)) {
            return tabletAliasStringMap.get(tabletAlias);
        }
        String uid = String.valueOf(tabletAlias.getUid());
        if (uid.length() < TEN_ZERO.length()) {
            uid = TEN_ZERO.substring(uid.length()) + uid;
        }
        String tabletAliasString = new StringBuilder(16).append(tabletAlias.getCell()).append("-").append(uid).toString();
        tabletAliasStringMap.putIfAbsent(tabletAlias, tabletAliasString);
        return tabletAliasString;
    }

    /**
     * @param aliasStr
     * @return
     * @throws TopoException
     */
    public static Topodata.TabletAlias parseTabletAlias(String aliasStr) throws TopoException {
        String[] nameParts = aliasStr.split("-");
        if (EXPECT_NAME_PARTS_LENGTH != nameParts.length) {
            throw TopoException.wrap("invalid tablet alias: '" + aliasStr + "', expecting format: '<cell>-<uid>'");
        }
        long uid = Long.parseLong(nameParts[1]);
        return Topodata.TabletAlias.newBuilder().setCell(nameParts[0]).setUid(uid).build();
    }

    /**
     * @param tablet
     * @return
     */
    public static String tabletToHumanString(final Topodata.Tablet tablet) {
        return tabletAliasString(tablet.getAlias()) + "[" + "keyspace: " + tablet.getKeyspace() + ", Shard: " + tablet.getShard() + ", Hostname: " + tablet.getHostname() + ",TabletType:" +
            tabletTypeLstring(tablet.getType()) + "]";
    }

    /**
     * @param tabletType
     * @return
     */
    public static String tabletTypeLstring(Topodata.TabletType tabletType) {
        return tabletTypeLowerName.containsKey(tabletType.getNumber()) ? tabletTypeLowerName.get(tabletType.getNumber())
            : Topodata.TabletType.UNRECOGNIZED.name();
    }

    public static String getPoolName(Topodata.Tablet tablet) {
        if (poolNameMap.containsKey(tablet)) {
            return poolNameMap.get(tablet);
        }
        String shardIp = tablet.getShard() + "@" + tablet.getHostname();
        poolNameMap.putIfAbsent(tablet, shardIp);
        return shardIp;
    }
}
