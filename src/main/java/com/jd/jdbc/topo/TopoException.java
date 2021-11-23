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

import io.vitess.proto.Vtrpc;
import lombok.Getter;

import static com.jd.jdbc.topo.TopoExceptionCode.BAD_VERSION;
import static com.jd.jdbc.topo.TopoExceptionCode.INTERRUPTED;
import static com.jd.jdbc.topo.TopoExceptionCode.NODE_EXISTS;
import static com.jd.jdbc.topo.TopoExceptionCode.NODE_NOT_EMPTY;
import static com.jd.jdbc.topo.TopoExceptionCode.NO_IMPLEMENTATION;
import static com.jd.jdbc.topo.TopoExceptionCode.NO_NODE;
import static com.jd.jdbc.topo.TopoExceptionCode.NO_UPDATE_NEEDED;
import static com.jd.jdbc.topo.TopoExceptionCode.PARTIAL_RESULT;
import static com.jd.jdbc.topo.TopoExceptionCode.TIMEOUT;
import static com.jd.jdbc.topo.TopoExceptionCode.UNKNOWN_CODE;

public class TopoException extends Exception {
    private Vtrpc.Code rpcCode;

    @Getter
    private TopoExceptionCode code;

    private String message;

    public TopoException(Vtrpc.Code rpcCode, String message) {
        super(message);
        this.rpcCode = rpcCode;
    }

    private TopoException(TopoExceptionCode code, String message) {
        super(message);
        this.code = code;
        this.message = message;
    }

    /**
     * @param errorMessage
     * @return
     */
    public static TopoException wrap(String errorMessage) {
        return wrap(UNKNOWN_CODE, errorMessage);
    }

    /**
     * @param topoExceptionCode
     * @param errorMessage
     * @return
     */
    public static TopoException wrap(TopoExceptionCode topoExceptionCode, String errorMessage) {
        TopoExceptionCode code;
        String message;
        switch (topoExceptionCode) {
            case NODE_EXISTS:
                code = NODE_EXISTS;
                message = String.format("node already exists: %s", errorMessage);
                break;
            case NO_NODE:
                code = NO_NODE;
                message = String.format("node doesn't exist: %s", errorMessage);
                break;
            case NODE_NOT_EMPTY:
                code = NODE_NOT_EMPTY;
                message = String.format("node not empty: %s", errorMessage);
                break;
            case TIMEOUT:
                code = TIMEOUT;
                message = String.format("deadline exceeded: %s", errorMessage);
                break;
            case INTERRUPTED:
                code = INTERRUPTED;
                message = String.format("interrupted: %s", errorMessage);
                break;
            case BAD_VERSION:
                code = BAD_VERSION;
                message = String.format("bad node version: %s", errorMessage);
                break;
            case PARTIAL_RESULT:
                code = PARTIAL_RESULT;
                message = String.format("partial result: %s", errorMessage);
                break;
            case NO_UPDATE_NEEDED:
                code = NO_UPDATE_NEEDED;
                message = String.format("no update needed: %s", errorMessage);
                break;
            case NO_IMPLEMENTATION:
                code = NO_IMPLEMENTATION;
                message = String.format("no such topology implementation %s", errorMessage);
                break;
            default:
                code = UNKNOWN_CODE;
                message = String.format("unknown topoExceptionCode: %s", errorMessage);
                break;
        }
        return new TopoException(code, message);
    }

    /**
     * @param rpcCode
     * @param errorMessage
     * @return
     */
    public static TopoException wrap(Vtrpc.Code rpcCode, String errorMessage) {
        return new TopoException(rpcCode, errorMessage);
    }

    /**
     * @param exception
     * @param topoExceptionCode
     * @return
     */
    public static Boolean isErrType(java.lang.Exception exception, TopoExceptionCode topoExceptionCode) {
        if (exception instanceof TopoException) {
            return ((TopoException) exception).code == topoExceptionCode;
        }
        return false;
    }

    /**
     * @return
     */
    @Override
    public String getMessage() {
        return this.message;
    }
}
