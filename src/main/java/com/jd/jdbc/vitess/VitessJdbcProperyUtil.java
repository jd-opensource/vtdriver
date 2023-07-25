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

import com.jd.jdbc.common.Constant;
import com.jd.jdbc.vitess.mysql.VitessPropertyKey;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import io.vitess.proto.Topodata;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

public class VitessJdbcProperyUtil {

    private VitessJdbcProperyUtil() {
    }

    public static void replaceLegacyPropertyValues(Properties info) {
        //all property keys are case-sensitive
        String zeroDateTimeBehavior = info.getProperty(VitessPropertyKey.ZERO_DATE_TIME_BEHAVIOR.getKeyName());
        if (zeroDateTimeBehavior != null && zeroDateTimeBehavior.equalsIgnoreCase("convertToNull")) {
            info.setProperty(VitessPropertyKey.ZERO_DATE_TIME_BEHAVIOR.getKeyName(), "CONVERT_TO_NULL");
        }
    }

    public static void addDefaultProperties(Properties info) {
        if (!info.containsKey(VitessPropertyKey.CHARACTER_ENCODING.getKeyName())) {
            info.setProperty(VitessPropertyKey.CHARACTER_ENCODING.getKeyName(), "utf-8");
        }
        if (!info.containsKey(VitessPropertyKey.SEND_FRACTIONAL_SECONDS.getKeyName())) {
            info.setProperty(VitessPropertyKey.SEND_FRACTIONAL_SECONDS.getKeyName(), "true");
        }
        if (!info.containsKey(VitessPropertyKey.TREAT_UTIL_DATE_AS_TIMESTAMP.getKeyName())) {
            info.setProperty(VitessPropertyKey.TREAT_UTIL_DATE_AS_TIMESTAMP.getKeyName(), "true");
        }
        if (!info.containsKey(VitessPropertyKey.USE_STREAM_LENGTHS_IN_PREP_STMTS.getKeyName())) {
            info.setProperty(VitessPropertyKey.USE_STREAM_LENGTHS_IN_PREP_STMTS.getKeyName(), "true");
        }
        if (!info.containsKey(VitessPropertyKey.AUTO_CLOSE_P_STMT_STREAMS.getKeyName())) {
            info.setProperty(VitessPropertyKey.AUTO_CLOSE_P_STMT_STREAMS.getKeyName(), "false");
        }
    }

    public static void checkCredentials(String path, Properties info) {
        if (!info.containsKey(VitessPropertyKey.USER.getKeyName()) || !info.containsKey(VitessPropertyKey.PASSWORD.getKeyName())) {
            throw new IllegalArgumentException("no user or password: '" + path + "'");
        }
    }

    public static void checkCell(Properties info) throws IllegalArgumentException {
        if (info.getProperty("cell") == null) {
            throw new IllegalArgumentException("no cell found in jdbc url");
        }
        String[] cells = info.getProperty("cell").split(",");
        if (cells.length < 1) {
            throw new IllegalArgumentException("no cell found in jdbc url");
        }
    }

    public static void checkSchema(String path) {
        if (path == null || !path.startsWith("/")) {
            throw new IllegalArgumentException("wrong database name path: '" + path + "'");
        }
    }

    public static void checkServerTimezone(Properties info) {
        String canonicalTimezone = info.getProperty(VitessPropertyKey.SERVER_TIMEZONE.getKeyName());
        if (canonicalTimezone == null) {
            throw new IllegalArgumentException("serverTimezone is not found in jdbc url");
        }
        if (!"GMT".equalsIgnoreCase(canonicalTimezone) && "GMT".equals(TimeZone.getTimeZone(canonicalTimezone).getID())) {
            throw new IllegalArgumentException("invalid serverTimezone in jdbc url");
        }
    }

    public static void checkCharacterEncoding(Properties properties) {
        String characterEncoding = properties.getProperty(VitessPropertyKey.characterEncoding.getKeyName());
        if (StringUtils.isEmpty(characterEncoding)) {
            throw new IllegalArgumentException("characterEncoding is not found in jdbc url");
        }
        String csn = Charset.defaultCharset().name();
        boolean characterEncodingFlag = "UTF-8".equalsIgnoreCase(characterEncoding) || "UTF8".equalsIgnoreCase(characterEncoding);
        boolean csnFlag = "UTF-8".equalsIgnoreCase(csn);
        if (characterEncodingFlag && csnFlag) {
            return;
        }
        throw new IllegalArgumentException("Only supports utf8 encoding, please check characterEncoding in jdbcurl and file.encoding in environment variable,characterEncoding = " + characterEncoding + ", file.encoding=" + csn);
    }

    public static String getDefaultKeyspace(Properties props) {
        List<String> keySpaces = Arrays.asList(props.getProperty(Constant.DRIVER_PROPERTY_SCHEMA).split(","));
        return keySpaces.get(0);
    }

    public static Topodata.TabletType getTabletType(Properties props) {
        String role = getRole(props);
        switch (role.toLowerCase()) {
            case Constant.DRIVER_PROPERTY_ROLE_RW:
                return Topodata.TabletType.MASTER;
            case Constant.DRIVER_PROPERTY_ROLE_RR:
                return Topodata.TabletType.REPLICA;
            case Constant.DRIVER_PROPERTY_ROLE_RO:
                return Topodata.TabletType.RDONLY;
            default:
                throw new IllegalArgumentException("'role=" + role + "' " + "error in jdbc url");
        }
    }

    public static String getRole(Properties props) {
        return props.getProperty(Constant.DRIVER_PROPERTY_ROLE_KEY, Constant.DRIVER_PROPERTY_ROLE_RW);
    }
}
