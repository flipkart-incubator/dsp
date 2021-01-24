package com.flipkart.dsp.entities.sg.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 */
@NoArgsConstructor
@EqualsAndHashCode
@Getter
@ToString(callSuper = true)
@Setter
public class DataFrameUnaryKey extends DataFrameKey implements Serializable{

    @JsonProperty("value")
    private String value;

    public DataFrameUnaryKey(DataFrameColumnType columnType, String value) {
        super(columnType);
        this.value = value;
    }
}
