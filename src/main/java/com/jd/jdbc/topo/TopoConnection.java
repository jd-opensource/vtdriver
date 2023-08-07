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

import com.jd.jdbc.context.IContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface TopoConnection extends Resource {
    /**
     * @param ctx
     * @param dirPath
     * @param isFull
     * @return
     * @throws TopoException
     */
    List<DirEntry> listDir(IContext ctx, String dirPath, boolean isFull, boolean withSerializable) throws TopoException;

    CompletableFuture<List<DirEntry>> listDirFuture(IContext ctx, String dirPath, Boolean isFull);

    /**
     * @param ctx
     * @param filePath
     * @param contents
     * @return
     * @throws TopoException
     */
    Version create(IContext ctx, String filePath, byte[] contents) throws TopoException;

    /**
     * @param ctx
     * @param filePath
     * @param ignoreNoNode
     * @return
     * @throws TopoException
     */
    ConnGetResponse get(IContext ctx, String filePath, boolean ignoreNoNode) throws TopoException;

    default ConnGetResponse get(IContext ctx, String filePath) throws TopoException {
        return get(ctx, filePath, false);
    }

    List<ConnGetResponse> getTabletsByCell(IContext ctx, String filePath) throws TopoException;

    CompletableFuture<ConnGetResponse> getFuture(IContext ctx, String filePath);

    void watchSrvKeyspace(IContext ctx, String cell, String keyspace) throws TopoException;

    enum DirEntryType {

        /**
         * TYPE_DIRECTORY
         */
        TYPE_DIRECTORY(0), TYPE_FILE(1);

        private int value;

        DirEntryType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }
    }

    interface Version {
        /**
         * @return
         */
        String string();
    }

    class DirEntry {
        private String name;

        private DirEntryType dirEntryType;

        private Boolean ephemeral;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public DirEntryType getDirEntryType() {
            return dirEntryType;
        }

        public void setDirEntryType(DirEntryType dirEntryType) {
            this.dirEntryType = dirEntryType;
        }

        public Boolean getEphemeral() {
            return ephemeral;
        }

        public void setEphemeral(Boolean ephemeral) {
            this.ephemeral = ephemeral;
        }
    }

    class ConnGetResponse {
        private byte[] contents;

        private Version version;

        public byte[] getContents() {
            return contents;
        }

        public void setContents(byte[] contents) {
            this.contents = contents;
        }

        public Version getVersion() {
            return version;
        }

        public void setVersion(Version version) {
            this.version = version;
        }
    }
}
