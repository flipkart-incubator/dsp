package com.flipkart.dsp.utils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.flipkart.dsp.entities.sg.dto.DataFrameKey;

import java.io.IOException;
import java.util.List;

/**
 */

public class DataFrameLinkedHashSetKeySerializer extends JsonSerializer<List<DataFrameKey>> {

    private ObjectMapper objectMapper = new ObjectMapper();
    public static final TypeReference keyDataType = new TypeReference<List<DataFrameKey>>() {
    };

    @Override
    public void serialize(List<DataFrameKey> linkedHashSet, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
        jsonGenerator.writeFieldName(objectMapper.writerFor(keyDataType).writeValueAsString(linkedHashSet));
    }
}
