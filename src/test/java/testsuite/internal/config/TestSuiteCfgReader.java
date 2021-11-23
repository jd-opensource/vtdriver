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

import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Properties;
import testsuite.internal.TestSuiteShardSpec;

public class TestSuiteCfgReader {
    private static final Log LOGGER = LogFactory.getLog(TestSuiteCfgReader.class);

    private static final String SET_METHOD_PREFIX = "set";

    private static final String DOT = ".";

    public static <T extends TestSuiteJdbcCfg> T read(Class<T> t, TestSuiteShardSpec shardSpec, TestSuiteCfgPath path) {
        T instance = null;
        try {
            instance = t.newInstance();

            Properties prop = read(path);

            Method method = t.getDeclaredMethod(T.ABSTRACT_GET_PREFIX_METHOD_NAME);
            method.setAccessible(true);
            String propPrefix = (String) method.invoke(instance);

            Field[] fields = t.getSuperclass().getDeclaredFields();
            setValues(t, shardSpec, instance, prop, propPrefix, fields);

            fields = t.getDeclaredFields();
            setValues(t, shardSpec, instance, prop, propPrefix, fields);
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage(), e);
        }
        return instance;
    }

    private static <T extends TestSuiteJdbcCfg> void setValues(Class<T> t, TestSuiteShardSpec shardSpec, T instance,
                                                               Properties prop, String propPrefix, Field[] fields) throws Exception {
        for (Field field : fields) {
            field.setAccessible(true);

            int modifiers = field.getModifiers();
            if (Modifier.isStatic(modifiers) || Modifier.isFinal(modifiers)) {
                continue;
            }

            String fieldName = field.getName();
            String propKey = convertCamalCaseToPropKey(fieldName);
            String propValue;
            if (prop.containsKey(propKey)) {
                propValue = prop.getProperty(propKey);
            } else if (prop.containsKey(propPrefix + DOT + propKey)) {
                propValue = prop.getProperty(propPrefix + DOT + propKey);
            } else {
                propValue = prop.getProperty(propPrefix + DOT + shardSpec.shardNumber + DOT + propKey);
            }

            Method setMethod = t.getMethod(getNameOfSetter(fieldName), String.class);
            setMethod.setAccessible(true);
            setMethod.invoke(instance, propValue);
        }
    }

    private static Properties read(TestSuiteCfgPath path) throws IOException {
        InputStream is = TestSuiteCfgReader.class.getClassLoader().getResourceAsStream(path.path);
        InputStreamReader isr = new InputStreamReader(Objects.requireNonNull(is), StandardCharsets.UTF_8);
        Properties prop = new Properties();
        prop.load(isr);
        return prop;
    }

    private static String convertCamalCaseToPropKey(String camalCase) {
        StringBuilder propKey = new StringBuilder();
        for (int i = 0; i < camalCase.length(); i++) {
            char c = camalCase.charAt(i);
            if (c >= 'A' && c <= 'Z') {
                propKey.append(DOT).append((char) (c + 32));
            } else {
                propKey.append(c);
            }
        }
        return propKey.toString();
    }

    private static String getNameOfSetter(String fieldName) {
        int num = fieldName.charAt(0) - 32;
        return SET_METHOD_PREFIX + ((char) num) + fieldName.substring(1);
    }
}
