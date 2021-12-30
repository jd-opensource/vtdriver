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

package com.jd.jdbc.discovery;

import com.jd.jdbc.queryservice.IHealthCheckQueryService;
import com.jd.jdbc.queryservice.IParentQueryService;
import com.jd.jdbc.queryservice.IQueryService;
import com.jd.jdbc.queryservice.TabletDialer;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.topo.topoproto.TopoProto;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.vitess.proto.Query;
import io.vitess.proto.Query.StreamHealthResponse;
import io.vitess.proto.Topodata;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import static com.jd.jdbc.discovery.TabletHealthCheck.TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_CANCELED;
import static com.jd.jdbc.discovery.TabletHealthCheck.TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_ERROR;
import static com.jd.jdbc.discovery.TabletHealthCheck.TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_ERROR_PACKET;
import static com.jd.jdbc.discovery.TabletHealthCheck.TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_MISMATCH;
import static com.jd.jdbc.discovery.TabletHealthCheck.TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_NONE;
import static com.jd.jdbc.discovery.TabletHealthCheck.TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_RESPONSE;

@Data
public class TabletHealthCheck {
    private static final Log log = LogFactory.getLog(HealthCheck.class);

    private static final String CTX_RESPONSE = "health check response ok";

    private static final String CTX_TABLET_STATUS_MISMATCH = "health check response stats mismatch";

    private final long healthCheckTimeout = 60;  //second

    @Getter
    private final AtomicBoolean serving;

    @Getter
    @Setter
    private final AtomicBoolean retrying;

    //    ITabletQueryService tabletQueryService;
    IParentQueryService queryService;

    ReentrantLock lock = new ReentrantLock();

    @Getter
    private AtomicReference<TabletStreamHealthDetailStatus> tabletStreamHealthDetailStatus =
        new AtomicReference<TabletStreamHealthDetailStatus>(new TabletStreamHealthDetailStatus(TABLET_STREAM_HEALTH_STATUS_NONE, ""));

    private volatile boolean isCanceled = false;

    private Query.Target target;

    private HealthCheck healthCheck;

    @Getter
    private Topodata.Tablet tablet;

    private Long masterTermStartTime;

    private ClientResponseObserver<Query.StreamHealthRequest, StreamHealthResponse> responseObserver;

    private ClientCallStreamObserver<Query.StreamHealthRequest> savedClientCallStreamObserver;

    private long lastResponseTimestamp;

    private Query.RealtimeStats stats;

    private AtomicReference<String> lastError;

    public TabletHealthCheck(HealthCheck healthCheck, Topodata.Tablet tablet, Query.Target target) {
        this.healthCheck = healthCheck;
        this.tablet = tablet;
        this.target = target;
        this.serving = new AtomicBoolean(false);
        this.retrying = new AtomicBoolean(false);
        this.lastError = new AtomicReference<>("");
    }

    public TabletHealthCheck simpleCopy() {
        return this;
    }

    public IQueryService getQueryService() {
        this.lock.lock();
        try {
            return (IQueryService) getTabletQueryServiceLocked();
        } finally {
            this.lock.unlock();
        }
    }

    public IHealthCheckQueryService getHealthCheckQueryService() {
        this.lock.lock();
        try {
            return (IHealthCheckQueryService) getTabletQueryServiceLocked();
        } finally {
            this.lock.unlock();
        }
    }

    private IParentQueryService getTabletQueryServiceLocked() {
        if (this.queryService == null) {
            this.queryService = TabletDialer.dial(this.tablet);
            log.info("create tablet query service: " + TopoProto.tabletToHumanString(tablet));
        }
        return this.queryService;
    }

    public void closeTabletQueryService(String err) {
        this.lastError.set(err);
        log.info("close tablet query service: " + TopoProto.tabletToHumanString(tablet) + ", due to error: " + err);
        finalizeConn();
    }

    public void finalizeConn() {
        this.lock.lock();
        try {
            this.serving.set(false);
            log.info("final close tablet query service: " + TopoProto.tabletToHumanString(tablet));

            if (savedClientCallStreamObserver != null) {
                savedClientCallStreamObserver.cancel("cancel stream health", null);
                savedClientCallStreamObserver = null;
            }

            // this.lastError todo set during checkConn
            if (this.queryService != null) {
                this.queryService.close();
                this.queryService = null;
            }
        } finally {
            this.lock.unlock();
        }
    }

    //topo 线程调用
    public void initStreamHealth() {
        this.lock.lock();
        responseObserver = new ClientResponseObserver<Query.StreamHealthRequest, StreamHealthResponse>() {
            @Override
            public void beforeStart(ClientCallStreamObserver<Query.StreamHealthRequest> clientCallStreamObserver) {
                savedClientCallStreamObserver = clientCallStreamObserver;
            }

            @Override
            public void onNext(StreamHealthResponse value) {
                handleStreamHealthResponse(value);
            }

            @Override
            public void onError(Throwable t) {
                handleStreamHealthError(t);
            }

            @Override
            public void onCompleted() {
                handleStreamHealthComplete();
            }
        };


        try {
            getHealthCheckQueryService().streamHealth(this, responseObserver);
        } catch (Exception e) {
            log.error("tablet " + TopoProto.tabletToHumanString(tablet) + "init stream health error, error msg : " + e.getMessage(), e);
            if (savedClientCallStreamObserver != null) {
                savedClientCallStreamObserver.cancel("streamHealth Exception", e);
                savedClientCallStreamObserver = null;
            }
            this.tabletStreamHealthDetailStatus.set(new TabletStreamHealthDetailStatus(TABLET_STREAM_HEALTH_STATUS_ERROR, "init stream health error, need restart stream health"));
        } finally {
            this.lock.unlock();
        }
    }

    public void startHealthCheckStream() {
        this.lock.lock();
        try {
            if (savedClientCallStreamObserver != null) {
                this.savedClientCallStreamObserver.cancel("stopHealthCheckStream", null);
                savedClientCallStreamObserver = null;
            }

            try {
                getHealthCheckQueryService().streamHealth(this, responseObserver);
            } catch (Exception e) {
                if (savedClientCallStreamObserver != null) {
                    savedClientCallStreamObserver.cancel("streamHealth Exception", e);
                    savedClientCallStreamObserver = null;
                }
                this.tabletStreamHealthDetailStatus.set(new TabletStreamHealthDetailStatus(TABLET_STREAM_HEALTH_STATUS_ERROR, "start stream health error, need restart stream health"));
            }
        } finally {
            this.lock.unlock();
        }
    }

    public void handleStreamHealthResponse(StreamHealthResponse response) {
        lock.lock();
        try {
            this.tabletStreamHealthDetailStatus.set(new TabletStreamHealthDetailStatus(TABLET_STREAM_HEALTH_STATUS_RESPONSE, ""));

            if (this.healthCheckCtxIsCanceled()) {
                return;
            }

            if (response.getTarget() == null || response.getRealtimeStats() == null) {
                this.tabletStreamHealthDetailStatus.set(new TabletStreamHealthDetailStatus(TABLET_STREAM_HEALTH_STATUS_ERROR_PACKET, "health stats is not valid " + response.toString()));
                return;
            }

            String healthError = null;
            boolean serving = response.getServing();

            if (!isEmptyStr(response.getRealtimeStats().getHealthError())) {
                healthError = "vttablet error: " + response.getRealtimeStats().getHealthError();
                serving = false;
            }

            if (response.getTabletAlias() != null && !TopoProto.tabletAliasEqual(response.getTabletAlias(), this.tablet.getAlias())) {
                this.tabletStreamHealthDetailStatus.set(new TabletStreamHealthDetailStatus(TABLET_STREAM_HEALTH_STATUS_MISMATCH,
                    "health stats mismatch, tablet " + this.tablet.getAlias() + " alias does not match response alias " + response.getTabletAlias()));
                //delete this :handleStreamHealthError already check this error
                //this.healthCheck.removeTablet(this.getTablet());
                return;
            }

            Query.Target prevTarget = this.target;

            boolean trivialUpdate = isEmptyStr(this.lastError.get()) &&
                this.serving.get() &&
                isEmptyStr(response.getRealtimeStats().getHealthError()) &&
                response.getServing() &&
                prevTarget.getTabletType() != Topodata.TabletType.MASTER &&
                prevTarget.getTabletType() == response.getTarget().getTabletType() &&
                this.isTrivialReplagChange(response.getRealtimeStats());
            this.lastResponseTimestamp = System.currentTimeMillis();
            this.target = response.getTarget();
            this.masterTermStartTime = response.getTabletExternallyReparentedTimestamp();
            this.stats = response.getRealtimeStats();
            this.lastError.set(healthError);

            this.serving.set(serving);

            if (serving) {
                this.retrying.set(false);
            }

            this.healthCheck.updateHealth(this.simpleCopy(), prevTarget, trivialUpdate, response.getServing());
        } finally {
            this.lock.unlock();
        }
    }

    public void handleStreamHealthError(Throwable t) {
        log.error("tablet " + TopoProto.tabletToHumanString(tablet) + " handleStreamHealthError error msg : " + t.getMessage());
        if (t.getMessage().toLowerCase().contains("health stats mismatch")) {
            //removeTablet 方法会调用 cancelHealthCheckCtx
            this.healthCheck.removeTablet(this.tablet);
            return;
        } else {
            if (!this.retrying.get()) {
                this.healthCheck.updateHealth(this.simpleCopy(), this.getTarget(), false, false);
            }
        }

        //close stream by client
        if (t.getMessage().contains("cancel stream health")) {
            log.info("tablet " + TopoProto.tabletToHumanString(tablet) + "cancel stream health " + this.tablet.getHostname());
        }

        this.tabletStreamHealthDetailStatus.set(new TabletStreamHealthDetailStatus(TABLET_STREAM_HEALTH_STATUS_ERROR, t.getMessage()));
    }

    //release the underlying connection resources.
    public void shutdown() {
        this.lock.lock();
        try {
            if (this.queryService != null) {
                this.queryService.close();
            }
        } finally {
            this.lock.unlock();
        }

        TabletDialer.close(tablet);
    }

    public void closeNativeQueryService() {
        if (this.queryService != null) {
            this.queryService.closeNativeQueryService();
        }
    }

    public void cancelHealthCheckCtx() {
        this.tabletStreamHealthDetailStatus.set(new TabletStreamHealthDetailStatus(TABLET_STREAM_HEALTH_STATUS_CANCELED, ""));
        this.isCanceled = true;
    }

    public boolean healthCheckCtxIsCanceled() {
        return this.tabletStreamHealthDetailStatus.get().status == TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_CANCELED;
    }

    public void handleStreamHealthComplete() {
        this.tabletStreamHealthDetailStatus.set(new TabletStreamHealthDetailStatus(TABLET_STREAM_HEALTH_STATUS_CANCELED, ""));
        this.isCanceled = true;
    }

    public boolean isEmptyStr(String str) {
        return null == str || str.isEmpty();
    }

    public boolean isTrivialReplagChange(Query.RealtimeStats stats) {
        if (this.stats == null) {
            return false;
        }

        int lowlag = 30;
        int oldlag = this.stats.getSecondsBehindMaster();
        int newlag = stats.getSecondsBehindMaster();

        if (oldlag < lowlag && newlag < lowlag) {
            return true;
        }
        return oldlag > lowlag && newlag > lowlag && newlag < oldlag * 1.1 && newlag > oldlag * 0.9;
    }

    public enum TabletStreamHealthStatus {
        TABLET_STREAM_HEALTH_STATUS_NONE,
        TABLET_STREAM_HEALTH_STATUS_CANCELED,
        TABLET_STREAM_HEALTH_STATUS_MISMATCH,
        TABLET_STREAM_HEALTH_STATUS_RESPONSE,
        TABLET_STREAM_HEALTH_STATUS_ERROR,
        TABLET_STREAM_HEALTH_STATUS_ERROR_PACKET
    }

    @Data
    @AllArgsConstructor
    public class TabletStreamHealthDetailStatus {
        TabletStreamHealthStatus status;

        String message;
    }
}
