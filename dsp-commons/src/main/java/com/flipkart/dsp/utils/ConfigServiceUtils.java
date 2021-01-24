package com.flipkart.dsp.utils;

import com.flipkart.dsp.exceptions.ConfigServiceException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

/**
 * +
 */
public class ConfigServiceUtils {

    private ConfigServiceUtils() {
        throw new IllegalStateException("ConfigServiceUtils class");
    }

    public static <T> T getConfig(Map<String, Object> configMap, String configClassName, Class<T> configClass) throws ConfigServiceException {
        try {
            T instance = configClass.getConstructor().newInstance();
            for (Field field : instance.getClass().getDeclaredFields()) {
                Object value = configMap.get(configClassName + "-" + field.getName());
                setFieldValue(instance, field, value);
            }
            return instance;
        } catch (Exception e) {
            String error = String.format("Error while desrialising config class object %s from Config Service", configClassName);
            throw new ConfigServiceException(error);
        }
    }

    private static <T> void setFieldValue(T instance, Field field, Object value) throws IOException, IllegalAccessException {
        if (value == null) {
            return;
        }

        if (field.getType().equals(Map.class)) {
            value = JsonUtils.DEFAULT.mapper.readValue((String) value, Map.class);
        }

        field.setAccessible(true);
        if (value instanceof Integer && field.getType().equals(Double.class)) {
            Integer value1 = (Integer) value;
            Double aDouble = new Double(value1);
            field.set(instance, aDouble);
        } else {
            field.set(instance, value);
        }
    }

}
