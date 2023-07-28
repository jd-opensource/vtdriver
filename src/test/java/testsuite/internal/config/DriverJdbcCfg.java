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

package testsuite.internal.config;

import java.util.StringJoiner;

public class DriverJdbcCfg extends TestSuiteJdbcCfg {
    protected String cell;

    protected String socketTimeout;

    public void setCell(String cell) {
        this.cell = cell;
    }

    public String getCell() {
        return this.cell;
    }

    public void setSocketTimeout(String socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    @Override
    public String getPrefix() {
        return "driver.jdbc";
    }

    @Override
    public String getJdbcUrl() {
        return String.format("%s/%s?user=%s&password=%s&serverTimezone=%s&characterEncoding=%s&socketTimeout=%s",
            this.urlPrefix, this.keyspace, this.username, this.password, this.serverTimezone,
            this.characterEncoding, this.socketTimeout);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DriverJdbcCfg.class.getSimpleName() + "[", "]")
            .add("cell='" + cell + "'")
            .add("characterEncoding='" + characterEncoding + "'")
            .add("serverTimezone='" + serverTimezone + "'")
            .add("urlPrefix='" + urlPrefix + "'")
            .add("keyspace='" + keyspace + "'")
            .add("username='" + username + "'")
            .add("password='" + password + "'")
            .toString();
    }
}
