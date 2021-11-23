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

package com.jd.jdbc.srvtopo;

import com.jd.jdbc.context.IContext;
import com.jd.jdbc.key.Destination;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import io.vitess.proto.Query;
import io.vitess.proto.Query.Target;
import io.vitess.proto.Query.Value;
import io.vitess.proto.Topodata;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

/**
 * A Resolver can resolve keyspace ids and key ranges into ResolvedShard*
 * objects. It uses an underlying srvtopo.Server to find the topology,
 * and a TargetStats object to find the healthy destinations.
 */
@Getter
public class Resolver {

    private static final String SINGLE_SHARD_REFERENCE_NAME = "0";

    private static final Log LOGGER = LogFactory.getLog(Resolver.class);

    private final SrvTopoServer srvTopoServer;

    private final Gateway gateway;

    private final String localCell;

    private final ScatterConn scatterConn;

    public Resolver(SrvTopoServer srvTopoServer, Gateway gateway, String localCell, ScatterConn scatterConn) {
        this.srvTopoServer = srvTopoServer;
        this.gateway = gateway;
        this.localCell = localCell;
        this.scatterConn = scatterConn;
    }

    /**
     * GetKeyspaceShards return all the shards in a keyspace. It follows
     * redirection if ServedFrom is set. It is only valid for the local cell.
     * Do not use it to further resolve shards, instead use the Resolve* methods.
     */
    public KeyspaceShardsResult getKeyspaceShards(IContext ctx, String keyspace, Topodata.TabletType tabletType) throws SQLException {
        Topodata.SrvKeyspace srvKeyspace;
        try {
            ResilientServer.GetSrvKeyspaceResponse response = srvTopoServer.getSrvKeyspace(ctx, localCell, keyspace);
            if (null != response.getException()) {
                LOGGER.error(response.getException().getMessage(), response.getException());
                throw response.getException();
            }
            srvKeyspace = response.getSrvKeyspace();
        } catch (Exception e) {
            throw new SQLException("keyspace %v fetch error: " + keyspace);
        }

        // check if the keyspace has been redirected for this tabletType.
        if (null != srvKeyspace) {
            for (Topodata.SrvKeyspace.ServedFrom sf : srvKeyspace.getServedFromList()) {
                if (sf.getTabletType() == tabletType) {
                    ResilientServer.GetSrvKeyspaceResponse response = srvTopoServer.getSrvKeyspace(ctx, localCell, sf.getKeyspace());
                    srvKeyspace = response.getSrvKeyspace();
                }
            }
        }
        if (null != srvKeyspace) {
            Topodata.SrvKeyspace.KeyspacePartition partition = srvKeyspaceGetPartition(srvKeyspace, tabletType);
            if (partition == null) {
                throw new SQLException(String.format("No partition found for tabletType %s in keyspace %s", tabletType.toString(), keyspace));
            }
            return new KeyspaceShardsResult(keyspace, srvKeyspace, partition.getShardReferencesList());
        } else {
            throw new SQLException("No SrvKeyspace " + keyspace);
        }
    }

    /**
     * GetAllShards returns the list of ResolvedShards associated with all
     * the shards in a keyspace.
     *
     * @param ctx
     * @param keyspace
     * @param tabletType
     * @return
     * @throws Exception
     */
    public AllShardResult getAllShards(IContext ctx, String keyspace, Topodata.TabletType tabletType) throws SQLException {
        KeyspaceShardsResult keyspaceShardsResult = getKeyspaceShards(ctx, keyspace, tabletType);
        List<ResolvedShard> res = new ArrayList<>(keyspaceShardsResult.getShardReferences().size());
        Target.Builder targetBuilder;
        ResolvedShard resolvedShard;
        for (Topodata.ShardReference shard : keyspaceShardsResult.getShardReferences()) {
            targetBuilder = Target.newBuilder().setKeyspace(keyspace).setShard(shard.getName()).setTabletType(tabletType)
                .setCell("");
            resolvedShard = new ResolvedShard();
            resolvedShard.setTarget(targetBuilder.build());
            resolvedShard.setGateway(gateway);
            res.add(resolvedShard);
        }
        return new AllShardResult(res, keyspaceShardsResult.getSrvKeyspace());
    }

    /**
     * ResolveDestinations resolves values and their destinations into their
     * respective shards.
     * If ids is nil, the returned [][]*querypb.Value is also nil.
     * Otherwise, len(ids) has to match len(destinations), and then the returned
     * [][]*querypb.Value is populated with all the values that go in each shard,
     * and len([]*ResolvedShard) matches len([][]*querypb.Value).
     * Sample input / output:
     * - destinations: dst1, dst2, dst3
     * - ids:          id1,  id2,  id3
     * If dst1 is in shard1, and dst2 aGetAllKeyspacesnd dst3 are in shard2, the output will be:
     * - []*ResolvedShard:   shard1, shard2
     * - [][]*querypb.Value: [id1],  [id2, id3]
     *
     * @param ctx
     * @param keyspace
     * @param tabletType
     * @param ids
     * @param destinations
     * @return
     * @throws Exception
     */
    public ResolveDestinationResult resolveDestinations(IContext ctx, String keyspace, Topodata.TabletType tabletType, List<Query.Value> ids, List<Destination> destinations) throws SQLException {
        KeyspaceShardsResult keyspaceShardsResult = getKeyspaceShards(ctx, keyspace, tabletType);
        List<Topodata.ShardReference> shardReferenceList = keyspaceShardsResult.getShardReferences();

        List<ResolvedShard> result = new ArrayList<>();
        List<List<Value>> values = new LinkedList<>();
        Map<String, Integer> resolved = new ConcurrentHashMap<>(16, 1);

        if (null != destinations && destinations.size() > 0) {
            for (int i = 0; i < destinations.size(); i++) {
                Destination destination = destinations.get(i);
                int finalI = i;
                destination.resolve(shardReferenceList, shard -> {
                    Integer s = resolved.get(shard);
                    if (null == s) {
                        Target.Builder targetBuilder = Target.newBuilder().setKeyspace(keyspace)
                            .setShard(shard)
                            .setTabletType(tabletType)
                            .setCell("");
                        s = result.size();
                        ResolvedShard resolvedShard = new ResolvedShard();
                        resolvedShard.setGateway(gateway);
                        resolvedShard.setTarget(targetBuilder.build());
                        result.add(resolvedShard);
                        if (ids != null) {
                            values.add(new ArrayList<>());
                        }
                        resolved.put(shard, s);
                    }
                    if (null != ids) {
                        List<Value> vs = values.get(s);
                        vs.add(ids.get(finalI));
                    }
                });
            }
        } else if (null != shardReferenceList && !shardReferenceList.isEmpty()) {
            // If there is only one shard and the name of the shard is "0", it is considered as SingleShard
            if (shardReferenceList.size() == 1
                && SINGLE_SHARD_REFERENCE_NAME.equalsIgnoreCase(shardReferenceList.get(0).getName())) {
                Target.Builder targetBuilder = Target.newBuilder()
                    .setKeyspace(keyspace)
                    .setShard(shardReferenceList.get(0).getName())
                    .setTabletType(tabletType)
                    .setCell("");
                ResolvedShard resolvedShard = new ResolvedShard();
                resolvedShard.setGateway(gateway);
                resolvedShard.setTarget(targetBuilder.build());
                result.add(resolvedShard);
            }
        }
        return new ResolveDestinationResult(result, values);
    }

    /**
     * ResolveDestination is a shortcut to ResolveDestinations with only one Destination, and no ids.
     *
     * @param ctx
     * @param keyspace
     * @param tabletType
     * @param destination
     * @return
     * @throws Exception
     */
    public List<ResolvedShard> resolveDestination(IContext ctx, String keyspace, Topodata.TabletType tabletType, Destination destination) throws Exception {
        ResolveDestinationResult resolveDestinationResult = this.resolveDestinations(ctx, keyspace, tabletType, null, new ArrayList<Destination>() {{
            add(destination);
        }});
        return resolveDestinationResult.getResolvedShards();
    }

    /**
     * @param sk
     * @param tabletType
     * @return
     */
    private Topodata.SrvKeyspace.KeyspacePartition srvKeyspaceGetPartition(Topodata.SrvKeyspace sk, Topodata.TabletType tabletType) {
        for (Topodata.SrvKeyspace.KeyspacePartition keyspacePartition : sk.getPartitionsList()) {
            if (keyspacePartition.getServedType() == tabletType) {
                return keyspacePartition;
            }
        }
        return null;
    }

    @Data
    private static class KeyspaceShardsResult {
        private String info;

        private Topodata.SrvKeyspace srvKeyspace;

        private List<Topodata.ShardReference> shardReferences;

        public KeyspaceShardsResult(String info, Topodata.SrvKeyspace srvKeyspace, List<Topodata.ShardReference> shardReferences) {
            this.info = info;
            this.srvKeyspace = srvKeyspace;
            this.shardReferences = shardReferences;
        }
    }

    @Data
    @AllArgsConstructor
    public static class AllShardResult {
        private List<ResolvedShard> resolvedShardList;

        private Topodata.SrvKeyspace srvKeyspace;
    }

    @Data
    @AllArgsConstructor
    public static class ResolveDestinationResult {
        private List<ResolvedShard> resolvedShards;

        private List<List<Value>> values;
    }
}
