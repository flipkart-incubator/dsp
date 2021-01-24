package com.flipkart.dsp.utils;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 */
@Slf4j
public class DataFrameLinkedHashSetKeyDeserializer extends KeyDeserializer {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Object deserializeKey(String s, DeserializationContext deserializationContext) {
        Object object = null;
        try {
            object = objectMapper.readValue(s, DataFrameLinkedHashSetKeySerializer.keyDataType);
        } catch (IOException e) {
            log.error("Exception while deserializing key", e);
        }
        return object;
    }
}
