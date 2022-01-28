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

package com.jd.jdbc.discovery;

import com.jd.jdbc.common.Constant;
import io.netty.util.internal.StringUtil;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

public enum SecurityCenter {
    INSTANCE;

    private Map<String, Credential> keySpaceCredentialMap = null;

    SecurityCenter() {
        keySpaceCredentialMap = new ConcurrentHashMap<>();
    }

    public void addCredential(Properties prop) throws SQLException {
        String[] keySpaces = prop.getProperty(Constant.DRIVER_PROPERTY_SCHEMA).split(",");
        for (String keySpace : keySpaces) {
            addCredential(keySpace, prop);
        }
    }

    public void addCredential(String keySpace, Properties prop) throws SQLException {
        String user = prop.getProperty("user");
        if (StringUtil.isNullOrEmpty(user)) {
            throw new SQLException("no user found for connection");
        }

        String password = prop.getProperty("password");
        if (StringUtil.isNullOrEmpty(password)) {
            throw new SQLException("no password found for connection");
        }
        Credential credentialNew = new Credential(user, password);
        Credential credential = keySpaceCredentialMap.putIfAbsent(keySpace, credentialNew);
        if (credential != null && !credential.equals(credentialNew)) {
            throw new SQLException("Cannot have a keyspace with the same name");
        }
    }

    public Credential getCredential(String keySpace) {
        Credential credential = keySpaceCredentialMap.get(keySpace);
        if (credential == null || StringUtil.isNullOrEmpty(credential.user) || StringUtil.isNullOrEmpty(credential.password)) {
            throw new IllegalArgumentException("no credential found for " + keySpace);
        }
        return credential;
    }

    @Getter
    @EqualsAndHashCode
    @AllArgsConstructor
    public class Credential {
        private final String user;

        private final String password;
    }
}
