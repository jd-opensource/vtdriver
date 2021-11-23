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

package com.jd.jdbc.queryservice;

import com.jd.jdbc.context.IContext;
import com.jd.jdbc.session.SafeSession;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtRowList;
import com.jd.jdbc.srvtopo.Resolver;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ShardQueryService {

    private static Query.Field[] fields;

    public static VtRowList handleShow(SafeSession safeSession, IContext context, String keyspace, Topodata.TabletType tabletType) throws SQLException {
        Resolver.AllShardResult rs = safeSession.getVitessConnection().getResolver().getAllShards(context, keyspace, tabletType);
        return shardMessage(rs, keyspace, tabletType);
    }

    private static VtRowList shardMessage(Resolver.AllShardResult shardResult, String keyspace, Topodata.TabletType tabletType) throws SQLException {
        VtResultSet rs = new VtResultSet();
        rs.setFields(getFields());
        rs.setRows(getRows(shardResult, keyspace, tabletType));
        return rs;
    }

    private static List<List<VtResultValue>> getRows(Resolver.AllShardResult shardResult, String keyspace, Topodata.TabletType tabletType) throws SQLException {
        for (Topodata.SrvKeyspace.KeyspacePartition partition : shardResult.getSrvKeyspace().getPartitionsList()) {
            if (partition.getServedType() != tabletType) {
                continue;
            }
            VtResultValue keySpaceVt = VtResultValue.newVtResultValue(Query.Type.VARCHAR, keyspace);
            List<List<VtResultValue>> rs = new ArrayList<>(partition.getShardReferencesCount());
            for (Topodata.ShardReference shardReference : partition.getShardReferencesList()) {
                List<VtResultValue> shard = new ArrayList<>();
                VtResultValue shardVt = VtResultValue.newVtResultValue(Query.Type.VARCHAR, shardReference.getName());
                shard.add(keySpaceVt);
                shard.add(shardVt);
                rs.add(shard);
            }
            return rs;
        }
        throw new SQLException("Shard message not found");
    }

    private static Query.Field[] getFields() {
        if (fields != null) {
            return fields;
        }
        synchronized (ShardQueryService.class) {
            if (fields != null) {
                return fields;
            }
            fields = new Query.Field[2];
            Query.Type type = Query.Type.VARCHAR;
            fields[0] = Query.Field.newBuilder().setName("keyspace").setType(type).build();
            fields[1] = Query.Field.newBuilder().setName("shard").setType(type).build();
            return fields;
        }
    }
}