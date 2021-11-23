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

public abstract class TestSuiteJdbcCfg {
    public static final String ABSTRACT_GET_PREFIX_METHOD_NAME = "getPrefix";

    protected String characterEncoding;

    protected String serverTimezone;

    protected String urlPrefix;

    protected String keyspace;

    protected String username;

    protected String password;

    public abstract String getPrefix();

    public abstract String getJdbcUrl();

    public void setCharacterEncoding(String characterEncoding) {
        this.characterEncoding = characterEncoding;
    }

    public void setServerTimezone(String serverTimezone) {
        this.serverTimezone = serverTimezone;
    }

    public void setUrlPrefix(String urlPrefix) {
        this.urlPrefix = urlPrefix;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", TestSuiteJdbcCfg.class.getSimpleName() + "[", "]")
            .add("characterEncoding='" + characterEncoding + "'")
            .add("serverTimezone='" + serverTimezone + "'")
            .add("urlPrefix='" + urlPrefix + "'")
            .add("keyspace='" + keyspace + "'")
            .add("username='" + username + "'")
            .add("password='" + password + "'")
            .toString();
    }
}
