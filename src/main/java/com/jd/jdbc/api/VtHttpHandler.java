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

package com.jd.jdbc.api;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static com.jd.jdbc.api.VtApiServerResponse.FAILURE;
import static com.jd.jdbc.api.VtApiServerResponse.SUCCESS;

public abstract class VtHttpHandler implements HttpHandler {

    /**
     * @param httpExchange
     * @throws IOException
     */
    protected void success(HttpExchange httpExchange) throws IOException {
        this.success(httpExchange, SUCCESS.getMessage());
    }

    /**
     * @param httpExchange
     * @param successMessage
     * @throws IOException
     */
    protected void success(HttpExchange httpExchange, String successMessage) throws IOException {
        try (OutputStream os = httpExchange.getResponseBody()) {
            httpExchange.sendResponseHeaders(SUCCESS.getCode(), successMessage.length());
            os.write(successMessage.getBytes(StandardCharsets.UTF_8));
            os.flush();
        }
    }

    /**
     * @param httpExchange
     * @throws IOException
     */
    protected void failure(HttpExchange httpExchange) throws IOException {
        this.failure(httpExchange, FAILURE.getMessage());
    }

    /**
     * @param httpExchange
     * @param errorMessage
     * @throws IOException
     */
    protected void failure(HttpExchange httpExchange, String errorMessage) throws IOException {
        try (OutputStream os = httpExchange.getResponseBody()) {
            httpExchange.sendResponseHeaders(FAILURE.getCode(), errorMessage.length());
            os.write(errorMessage.getBytes(StandardCharsets.UTF_8));
            os.flush();
        }
    }
}
