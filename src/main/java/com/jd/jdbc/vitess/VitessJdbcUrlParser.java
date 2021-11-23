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

package com.jd.jdbc.vitess;

import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.sqlparser.utils.Utils;
import com.jd.jdbc.vitess.mysql.VitessPropertyKey;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import static com.jd.jdbc.vitess.VitessJdbcProperyUtil.addDefaultProperties;
import static com.jd.jdbc.vitess.VitessJdbcProperyUtil.checkCell;
import static com.jd.jdbc.vitess.VitessJdbcProperyUtil.checkCredentials;
import static com.jd.jdbc.vitess.VitessJdbcProperyUtil.checkSchema;
import static com.jd.jdbc.vitess.VitessJdbcProperyUtil.replaceLegacyPropertyValues;

public class VitessJdbcUrlParser {

    public static final String JDBC_PREFIX = "jdbc:";

    public static final String JDBC_VITESS_PREFIX = JDBC_PREFIX + "vitess:";

    public static Properties parse(String url, Properties info) {
        if (!url.startsWith(JDBC_VITESS_PREFIX)) {
            throw new IllegalArgumentException("'" + JDBC_VITESS_PREFIX + "' prefix is mandatory");
        }

        Properties parsedProperties = new Properties();
        if (null != info && !info.isEmpty()) {
            info.forEach((k, v) -> {
                if (null != k && null != v) {
                    parsedProperties.setProperty(String.valueOf(k), String.valueOf(v));
                }
            });
        }
        URI uri;
        try {
            uri = new URI(url.substring(JDBC_VITESS_PREFIX.length()));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        parsedProperties.setProperty("host", uri.getHost());
        parsedProperties.setProperty("port", String.valueOf(uri.getPort()));

        String path = uri.getPath();
        checkSchema(path);

        parsedProperties.setProperty("schema", path.substring(1));

        String parameters = uri.getQuery();
        parameters = StringUtils.replaceEach(parameters, new String[] {":", "&nbsp;"}, new String[] {"", " "});
        String[] parameterPairs = parameters.split("&");
        for (String parameterPair : parameterPairs) {
            parsedProperties.setProperty(parameterPair.split("=")[0], parameterPair.split("=")[1]);
        }

        checkCell(parsedProperties);
        checkCredentials(path, parsedProperties);
        addDefaultProperties(parsedProperties);
        replaceLegacyPropertyValues(parsedProperties);

        Integer socketTimeout = null;
        if (parsedProperties.containsKey(VitessPropertyKey.SOCKET_TIMEOUT.getKeyName())) {
            socketTimeout = Utils.getInteger(parsedProperties, VitessPropertyKey.SOCKET_TIMEOUT.getKeyName());
            if (socketTimeout != null && socketTimeout < 1000) {
                socketTimeout = 1000;
            }
        }
        parsedProperties.put(VitessPropertyKey.SOCKET_TIMEOUT.getKeyName(), String.valueOf(socketTimeout == null ? 10000 : socketTimeout));
        parsedProperties.put(VitessPropertyKey.ALLOW_MULTI_QUERIES.getKeyName(), "true");

        return parsedProperties;
    }
}
