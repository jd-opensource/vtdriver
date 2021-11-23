package com.jd.jdbc.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import java.io.IOException;
import java.text.SimpleDateFormat;

public class JsonUtil {
    private static final Log log = LogFactory.getLog(JsonUtil.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.ALWAYS);
        OBJECT_MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        OBJECT_MAPPER.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
        OBJECT_MAPPER.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        OBJECT_MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        OBJECT_MAPPER.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
        OBJECT_MAPPER.enable(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT);
        OBJECT_MAPPER.enable(JsonParser.Feature.ALLOW_COMMENTS);
        OBJECT_MAPPER.enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES);
        OBJECT_MAPPER.enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES);
    }

    private JsonUtil() {
    }

    public static String toJSONString(Object object, boolean pretty) {
        String jsonStr = null;
        try {
            if (pretty) {
                jsonStr = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(object);
            } else {
                if (object instanceof String) {
                    JsonNode jsonNode = OBJECT_MAPPER.readTree(String.valueOf(object));
                    jsonStr = OBJECT_MAPPER.writeValueAsString(jsonNode);
                } else {
                    jsonStr = OBJECT_MAPPER.writeValueAsString(object);
                }
            }
        } catch (IOException e) {
            log.error("toJSONString error", e);
        }
        return jsonStr;
    }

    public static String toJSONString(Object object) {
        return toJSONString(object, false);
    }

    public static <T> T parseObject(Object object, Class<T> clzz) {
        String text = toJSONString(object);
        T t = null;
        if (text != null) {
            try {
                t = OBJECT_MAPPER.readValue(text, clzz);
            } catch (IOException e) {
                log.error("parseObject error", e);
            }
        }
        return t;
    }
}