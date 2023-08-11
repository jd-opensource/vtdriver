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

package com.jd.jdbc.queryservice;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.vitess.proto.Query;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import lombok.Getter;
import queryservice.QueryGrpc;

public class MockQueryServer extends QueryGrpc.QueryImplBase {

    private static final ExecutorService pool = new ThreadPoolExecutor(10, 10,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(),
        new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(final Runnable r) {
                return new Thread(r, threadNumber.getAndIncrement() + "");
            }
        });

    private final BlockingQueue<HealthCheckMessage> streamHealthMessage;

    private final List<StreamObserver<Query.StreamHealthResponse>> observers;

    @Getter
    private int connectCount;

    public MockQueryServer(BlockingQueue<HealthCheckMessage> streamHealthMessage) {
        this.streamHealthMessage = streamHealthMessage;
        this.observers = new ArrayList<>();
        pool.execute(this::subscribe);

        this.connectCount = 0;
    }

    private void subscribe() {
        while (true) {
            try {
                HealthCheckMessage message = streamHealthMessage.take();
                notifyAll(message);
                if (message.getMessageType() == MessageType.Close) {
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    private void notifyAll(HealthCheckMessage message) {
        switch (message.getMessageType()) {
            case Close:
                observers.forEach(StreamObserver::onCompleted);
                System.out.println("server: receive an ending message, complete stream connection");
                observers.clear();
                break;
            case Error:
                observers.forEach(observer -> {
                    observer.onError(new StatusRuntimeException(Status.UNKNOWN));
                });
                System.out.println("server: sending an error message");
                observers.clear();
                break;
            default:
                observers.forEach(observer -> {
                    try {
                        observer.onNext(message.getMessage());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                System.out.printf("server: receive message:  keyspace:%s, shard:%s, tablet_type:%s, cell:%s, uid:%s\n", message.getMessage().getTarget().getKeyspace(), message.getMessage().getTarget().getShard(),
                    message.getMessage().getTarget().getTabletType(), message.getMessage().getTabletAlias().getCell(), message.getMessage().getTabletAlias().getUid());
        }
    }

    @Override
    public void streamHealth(Query.StreamHealthRequest request, StreamObserver<Query.StreamHealthResponse> responseObserver) {
        System.out.println("server: build a health check connection");
        this.observers.add(responseObserver);
        this.connectCount++;
    }

    public enum MessageType {
        Next,
        Error,
        Close,
    }

    @AllArgsConstructor
    @Getter
    public static class HealthCheckMessage {

        private final MessageType messageType;

        private final Query.StreamHealthResponse message;
    }
}
