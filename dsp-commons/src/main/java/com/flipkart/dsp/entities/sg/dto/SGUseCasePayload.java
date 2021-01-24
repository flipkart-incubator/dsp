package com.flipkart.dsp.entities.sg.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.flipkart.dsp.utils.DataFrameLinkedHashSetKeyDeserializer;
import com.flipkart.dsp.utils.DataFrameLinkedHashSetKeySerializer;
import lombok.*;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


/**
 */
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Getter
@Setter
public class SGUseCasePayload implements Serializable {

    @JsonProperty("request_id")
    private Long requestId;

    @JsonProperty("data_frame_id")
    private String dataFrameId;

    @JsonProperty("column_metadata")
    private LinkedHashMap<String, DataFrameColumnType> columnMetaData;

    @JsonProperty("dataframes")
    @JsonSerialize(keyUsing = DataFrameLinkedHashSetKeySerializer.class)
    @JsonDeserialize(keyUsing = DataFrameLinkedHashSetKeyDeserializer.class)
    private Map<List<DataFrameKey>, Set<String>> dataframes;

    public void addColumnNameToDataFrameKeys() {
        List<String> columnNames = new ArrayList<>(columnMetaData.keySet());

        AtomicInteger i = new AtomicInteger();
        final int columnsSize = columnNames.size();
        dataframes.keySet().stream().flatMap(Collection::stream).forEach(dfKey -> {
            String columnName = columnNames.get(i.getAndIncrement());
            if (i.get() == columnsSize) {
                i.set(0);
            }
            dfKey.setName(columnName);
        });
    }
}
