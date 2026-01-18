package com.bdir.dps.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.TypeReference;
import com.alibaba.fastjson2.JSONWriter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

/**
 * JSON工具类
 * 基于FastJSON2封装，提供JSON序列化和反序列化功能
 */
@Slf4j
public class JsonUtil {

    /**
     * 将对象转换为JSON字符串
     *
     * @param object 要转换的对象
     * @return JSON字符串
     */
    public static String toJson(Object object) {
        try {
            return JSON.toJSONString(object);
        } catch (Exception e) {
            log.error("Convert object to JSON error", e);
            throw new RuntimeException("JSON serialization error", e);
        }
    }

    /**
     * 将对象转换为格式化的JSON字符串
     *
     * @param object 要转换的对象
     * @return 格式化的JSON字符串
     */
    public static String toJsonPretty(Object object) {
        try {
            return JSON.toJSONString(object, JSONWriter.Feature.PrettyFormat);
        } catch (Exception e) {
            log.error("Convert object to pretty JSON error", e);
            throw new RuntimeException("JSON serialization error", e);
        }
    }

    /**
     * 将JSON字符串转换为指定类型的对象
     *
     * @param json  JSON字符串
     * @param clazz 目标类型
     * @param <T>   目标类型
     * @return 转换后的对象
     */
    public static <T> T fromJson(String json, Class<T> clazz) {
        try {
            return JSON.parseObject(json, clazz);
        } catch (Exception e) {
            log.error("Convert JSON to object error, json: {}", json, e);
            throw new RuntimeException("JSON deserialization error", e);
        }
    }

    /**
     * 将JSON字符串转换为指定类型的对象
     *
     * @param json          JSON字符串
     * @param typeReference 类型引用
     * @param <T>           目标类型
     * @return 转换后的对象
     */
    public static <T> T fromJson(String json, TypeReference<T> typeReference) {
        try {
            return JSON.parseObject(json, typeReference);
        } catch (Exception e) {
            log.error("Convert JSON to object error, json: {}", json, e);
            throw new RuntimeException("JSON deserialization error", e);
        }
    }

    /**
     * 将JSON字符串转换为List
     *
     * @param json  JSON字符串
     * @param clazz 元素类型
     * @param <T>   元素类型
     * @return List对象
     */
    public static <T> List<T> fromJsonArray(String json, Class<T> clazz) {
        try {
            return JSON.parseArray(json, clazz);
        } catch (Exception e) {
            log.error("Convert JSON array to list error, json: {}", json, e);
            throw new RuntimeException("JSON deserialization error", e);
        }
    }

    /**
     * 将JSON字符串转换为Map
     *
     * @param json JSON字符串
     * @return Map对象
     */
    public static Map<String, Object> fromJsonToMap(String json) {
        try {
            return JSON.parseObject(json, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            log.error("Convert JSON to map error, json: {}", json, e);
            throw new RuntimeException("JSON deserialization error", e);
        }
    }

    /**
     * 将对象转换为JSONObject
     *
     * @param object 要转换的对象
     * @return JSONObject
     */
    public static JSONObject toJsonObject(Object object) {
        try {
            return JSON.parseObject(toJson(object));
        } catch (Exception e) {
            log.error("Convert object to JSONObject error", e);
            throw new RuntimeException("JSON serialization error", e);
        }
    }

    /**
     * 将JSON字符串转换为JSONObject
     *
     * @param json JSON字符串
     * @return JSONObject
     */
    public static JSONObject parseObject(String json) {
        try {
            return JSON.parseObject(json);
        } catch (Exception e) {
            log.error("Parse JSON object error, json: {}", json, e);
            throw new RuntimeException("JSON parse error", e);
        }
    }

    /**
     * 将JSON字符串转换为JSONArray
     *
     * @param json JSON字符串
     * @return JSONArray
     */
    public static JSONArray parseArray(String json) {
        try {
            return JSON.parseArray(json);
        } catch (Exception e) {
            log.error("Parse JSON array error, json: {}", json, e);
            throw new RuntimeException("JSON parse error", e);
        }
    }

    /**
     * 判断字符串是否为有效的JSON格式
     *
     * @param json 要判断的字符串
     * @return true表示是有效的JSON格式
     */
    public static boolean isValidJson(String json) {
        if (json == null || json.trim().isEmpty()) {
            return false;
        }
        try {
            JSON.parse(json);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 判断字符串是否为有效的JSON对象格式
     *
     * @param json 要判断的字符串
     * @return true表示是有效的JSON对象格式
     */
    public static boolean isValidJsonObject(String json) {
        if (json == null || json.trim().isEmpty()) {
            return false;
        }
        try {
            JSON.parseObject(json);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 判断字符串是否为有效的JSON数组格式
     *
     * @param json 要判断的字符串
     * @return true表示是有效的JSON数组格式
     */
    public static boolean isValidJsonArray(String json) {
        if (json == null || json.trim().isEmpty()) {
            return false;
        }
        try {
            JSON.parseArray(json);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}