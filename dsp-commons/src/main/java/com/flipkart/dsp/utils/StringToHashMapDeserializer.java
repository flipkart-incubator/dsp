package com.flipkart.dsp.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.util.Map;

/**
 */

public class StringToHashMapDeserializer extends JsonDeserializer<Map<String, String>> {

    @Override
    public Map<String, String> deserialize(JsonParser p, DeserializationContext context) throws IOException {
        String content = p.getText();
        return JsonUtils.DEFAULT.fromJson(content, new TypeReference<Map<String, String>>() {
        });
    }
}
