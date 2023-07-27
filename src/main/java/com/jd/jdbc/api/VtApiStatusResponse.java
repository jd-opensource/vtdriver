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

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class VtApiStatusResponse {

    private final String status;

    private final String statusUrl;

    private final String refreshVschemaUrl;

    private final String keyspaces;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"status\":\"")
            .append(status).append('\"');
        sb.append(",\"statusUrl\":\"")
            .append(statusUrl).append('\"');
        sb.append(",\"refreshVschemaUrl\":\"")
            .append(refreshVschemaUrl).append('\"');
        sb.append(",\"keyspaces\":\"")
            .append(keyspaces).append('\"');
        sb.append('}');
        return sb.toString();
    }
}
