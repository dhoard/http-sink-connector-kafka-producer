package com.github.dhoard.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsonorg.JsonOrgModule;
import org.json.JSONArray;
import org.json.JSONObject;

public class JSONUtil {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final ObjectMapper objectMapperPretty = new ObjectMapper();

    static {
        objectMapper.registerModule(new JsonOrgModule());
        objectMapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

        objectMapperPretty.registerModule(new JsonOrgModule());
        objectMapperPretty.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        objectMapperPretty.configure(SerializationFeature.INDENT_OUTPUT, true);
    }

    public static JSONObject parseJSONObject(String json) {
        JSONObject result = null;

        try {
            result = objectMapper.readValue(json, JSONObject.class);
        } catch (Throwable t) {
            ThrowableUtil.throwUnchecked(t);
        }

        return result;
    }

    public static JSONArray parseJSONArray(String json) {
        JSONArray jsonArray = null;

        try {
            jsonArray = objectMapper.readValue(json, JSONArray.class);
        } catch (Throwable t) {
            ThrowableUtil.throwUnchecked(t);
        }

        return jsonArray;
    }

    public static String toJSON(JSONObject jsonObject) {
        return toJSON(jsonObject, false);
    }

    public static String toJSON(JSONObject jsonObject, boolean pretty) {
        String result = null;
        ObjectMapper objectMapper = JSONUtil.objectMapper;

        if (pretty) {
            objectMapper = JSONUtil.objectMapperPretty;
        }

        try {
            result = objectMapper.writeValueAsString(jsonObject);
        } catch (Throwable t) {
            ThrowableUtil.throwUnchecked(t);
        }

        return result;
    }

    public static String toJSON(JSONArray jsonArray) {
        return toJSON(jsonArray, false);
    }

    public static String toJSON(JSONArray jsonArray, boolean pretty) {
        String result = null;
        ObjectMapper objectMapper = JSONUtil.objectMapper;

        if (pretty) {
            objectMapper = JSONUtil.objectMapperPretty;
        }

        try {
            result = objectMapper.writeValueAsString(jsonArray);
        } catch (Throwable t) {
            ThrowableUtil.throwUnchecked(t);
        }

        return result;
    }
}
