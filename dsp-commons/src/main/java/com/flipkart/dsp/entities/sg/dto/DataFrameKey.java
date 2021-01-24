package com.flipkart.dsp.entities.sg.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DataFrameUnaryKey.class, name = "uniValue"),
        @JsonSubTypes.Type(value = DataFrameBinaryKey.class, name = "biValue"),
        @JsonSubTypes.Type(value = DataFrameMultiKey.class, name = "multiValue")
})
@NoArgsConstructor
@EqualsAndHashCode
@ToString
@Getter
public abstract class DataFrameKey implements Serializable {

    @JsonProperty("column_type")
    private DataFrameColumnType columnType;

    @JsonIgnore
    @Setter
    private String name;

    public DataFrameKey(DataFrameColumnType columnType) {
        this.columnType = columnType;
    }
}
