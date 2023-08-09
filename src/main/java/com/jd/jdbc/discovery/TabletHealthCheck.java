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
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.topo.topoproto.TopoProto;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.vitess.proto.Query;
import io.vitess.proto.Query.StreamHealthResponse;
import io.vitess.proto.Topodata;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

public class TabletHealthCheck {
    private static final Log log = LogFactory.getLog(TabletHealthCheck.class);

    private static final String STOP_HEALTH_CHECK_STREAM = "stopHealthCheckStream";

    /**
     * 60s
     */
    @Getter
    private final long healthCheckTimeout = 60000;

    @Getter
    private final AtomicBoolean serving;

    @Getter
    @Setter
    private AtomicBoolean retrying;

    private IParentQueryService queryService;

    private ReentrantLock lock = new ReentrantLock();

    @Getter
    private AtomicReference<TabletStreamHealthDetailStatus> tabletStreamHealthDetailStatus =
        new AtomicReference<>(new TabletStreamHealthDetailStatus(TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_NONE, null));

    @Getter
    private Query.Target target;

    private HealthCheck healthCheck;

    @Getter
    private Topodata.Tablet tablet;

    @Getter
    private Long masterTermStartTime;

    private ClientResponseObserver<Query.StreamHealthRequest, StreamHealthResponse> responseObserver;

    private ClientCallStreamObserver<Query.StreamHealthRequest> savedClientCallStreamObserver;

    @Getter
    private long lastResponseTimestamp;

    @Getter
    private Query.RealtimeStats stats;

    @Getter
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

    private IHealthCheckQueryService getHealthCheckQueryService() {
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
            if (log.isDebugEnabled()) {
                log.debug("create tablet query service: " + TopoProto.tabletToHumanString(tablet));
            }
        }
        return this.queryService;
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
        } catch (Throwable e) {
            log.error("tablet " + TopoProto.tabletToHumanString(tablet) + "init stream health error, error msg : " + e.getMessage(), e);
            if (savedClientCallStreamObserver != null) {
                savedClientCallStreamObserver.cancel("streamHealth Exception", e);
                savedClientCallStreamObserver = null;
            }
            this.setStatus(TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_ERROR, "init stream health error, need restart stream health");
        } finally {
            this.lock.unlock();
        }
    }

    public void startHealthCheckStream() {
        this.lock.lock();
        try {
            if (savedClientCallStreamObserver != null) {
                this.savedClientCallStreamObserver.cancel(STOP_HEALTH_CHECK_STREAM, null);
                savedClientCallStreamObserver = null;
            }

            try {
                getHealthCheckQueryService().streamHealth(this, responseObserver);
            } catch (Throwable e) {
                if (savedClientCallStreamObserver != null) {
                    savedClientCallStreamObserver.cancel("streamHealth Exception", e);
                    savedClientCallStreamObserver = null;
                }
                this.setStatus(TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_ERROR, "start stream health error, need restart stream health");
            }
        } finally {
            this.lock.unlock();
        }
    }

    private void handleStreamHealthResponse(StreamHealthResponse response) {
        this.lock.lock();
        try {
            if (this.healthCheckCtxIsCanceled()) {
                return;
            }
            if (response == null) {
                this.setStatus(TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_ERROR_PACKET, "health stats is not valid ");
                return;
            }
            if (Objects.equals(Query.Target.getDefaultInstance(), response.getTarget())) {
                this.setStatus(TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_ERROR_PACKET, "health stats is not valid " + response);
                return;
            }
            this.setStatus(TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_RESPONSE, null);

            String healthError = null;
            boolean serving = response.getServing();

            if (StringUtils.isNotEmpty(response.getRealtimeStats().getHealthError())) {
                healthError = "vttablet error: " + response.getRealtimeStats().getHealthError();
                serving = false;
            }

            if (!TopoProto.tabletAliasEqual(response.getTabletAlias(), this.tablet.getAlias())) {
                this.setStatus(TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_MISMATCH,
                    "health stats mismatch, tablet " + this.tablet.getAlias() + " alias does not match response alias " + response.getTabletAlias());
                //delete this :handleStreamHealthError already check this error
                this.serving.set(false);
                return;
            }

            Query.Target prevTarget = this.target;

            boolean trivialUpdate = StringUtils.isEmpty(this.lastError.get()) && this.serving.get() && StringUtils.isEmpty(response.getRealtimeStats().getHealthError()) && response.getServing()
                && prevTarget.getTabletType() != Topodata.TabletType.MASTER && prevTarget.getTabletType() == response.getTarget().getTabletType()
                && this.isTrivialReplagChange(response.getRealtimeStats());
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

    private void handleStreamHealthError(Throwable t) {
        this.serving.set(false);
        log.error("tablet " + TopoProto.tabletToHumanString(tablet) + " handleStreamHealthError error msg : " + t.getMessage());
        if (t.getMessage().toLowerCase().contains("health stats mismatch")) {
            this.healthCheck.removeTablet(this.tablet);
            return;
        } else if (t.getMessage().contains(STOP_HEALTH_CHECK_STREAM)) {
            return;
        } else {
            boolean logFlag = false;
            if (t instanceof StatusRuntimeException) {
                StatusRuntimeException statusRuntimeException = (StatusRuntimeException) t;
                Status status = statusRuntimeException.getStatus();
                logFlag = "channel closed".equals(status.getDescription())
                    || "io exception".equals(status.getDescription())
                    || "Keepalive failed. The connection is likely gone".equals(status.getDescription())
                    || "Network closed for unknown reason".equals(status.getDescription());
            }
            if (!logFlag) {
                log.error("tablet " + TopoProto.tabletToHumanString(tablet) + " handleStreamHealthError error msg : ", t);
            }
            if (!this.retrying.get()) {
                this.healthCheck.updateHealth(this.simpleCopy(), this.getTarget(), false, false);
            }
        }

        //close stream by client
        if (t.getMessage().contains("cancel stream health")) {
            log.info("tablet " + TopoProto.tabletToHumanString(tablet) + "cancel stream health " + this.tablet.getHostname());
        }
        this.setStatus(TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_ERROR, t.getMessage());
    }

    //release the underlying connection resources.
    public void shutdown() {
        this.serving.set(false);
        this.lock.lock();
        try {
            if (savedClientCallStreamObserver != null) {
                this.savedClientCallStreamObserver.cancel(STOP_HEALTH_CHECK_STREAM, null);
                savedClientCallStreamObserver = null;
            }
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
        this.setStatus(TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_CANCELED, null);
    }

    public boolean healthCheckCtxIsCanceled() {
        return Objects.equals(this.tabletStreamHealthDetailStatus.get().getStatus(), TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_CANCELED);
    }

    private void handleStreamHealthComplete() {
        this.setStatus(TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_CANCELED, null);
    }

    private boolean isTrivialReplagChange(Query.RealtimeStats stats) {
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

    private void setStatus(TabletStreamHealthStatus status, String message) {
        TabletStreamHealthStatus preStatus = this.tabletStreamHealthDetailStatus.get().getStatus();
        if (Objects.equals(preStatus, TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_CANCELED)) {
            log.error("setStatus error preStatus=" + preStatus + " targetStatus=" + status);
            return;
        }
        if (Objects.equals(preStatus, status)) {
            return;
        }
        if (Objects.equals(status, TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_NONE)) {
            return;
        }
        this.tabletStreamHealthDetailStatus.set(new TabletStreamHealthDetailStatus(status, message));
    }

    @AllArgsConstructor
    @Getter
    public static class TabletStreamHealthDetailStatus {
        private final TabletStreamHealthStatus status;

        private final String message;
    }
}
