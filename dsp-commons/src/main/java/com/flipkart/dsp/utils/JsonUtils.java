package com.flipkart.dsp.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.dropwizard.jackson.Jackson;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 */

public enum JsonUtils {

    DEFAULT(getMapper()),
    PRETTY(getMapper().enable(SerializationFeature.INDENT_OUTPUT));

    public final ObjectMapper mapper;

    JsonUtils(ObjectMapper mapper) {
        this.mapper = mapper;
        mapper.findAndRegisterModules();
    }

    public <T> T convertValue(Object value, Class<T> clazz) {
        return mapper.convertValue(value, clazz);
    }

    public <T> String toJson(T object) {
        try {
            return mapper.writeValueAsString(object);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T fromJson(String jsonString, Class<T> clazz) {
        try {
            return mapper.readValue(jsonString, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T fromJson(String jsonString, Class<T> clazz, String errorMessage) {
        try {
            return mapper.readValue(jsonString, clazz);
        } catch (IOException e) {
            throw new RuntimeException(errorMessage, e);
        }
    }


    public <T> T fromJson(String jsonString, TypeReference<T> typeReference) {
        try {
            return mapper.readValue(jsonString, typeReference);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T fromJson(String jsonString, TypeReference<T> typeReference, String errorMessage) {
        try {
            return mapper.readValue(jsonString, typeReference);
        } catch (IOException e) {
            throw new RuntimeException(errorMessage, e);
        }
    }


    public <T> T fromJson(byte[] json, Class<T> clazz) {
        try {
            return mapper.readValue(json, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T fromJson(byte[] json, TypeReference<T> typeReference) {
        try {
            return mapper.readValue(json, typeReference);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, Object> asMap(String jsonString) {
        try {
            final TypeReference<LinkedHashMap<String, Object>> typeRef = new TypeReference<LinkedHashMap<String, Object>>() {};
            return mapper.readValue(jsonString, typeRef);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static ObjectMapper getMapper() {
        ObjectMapper mapper = Jackson.newObjectMapper();
        mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd"));
        mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        return mapper;
    }

}

