package com.flipkart.dsp.entities.sg.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.io.Serializable;

/**
 */

@NoArgsConstructor
@EqualsAndHashCode
@Getter
@ToString(callSuper = true)
@Setter
public class DataFrameBinaryKey extends DataFrameKey implements Serializable {
    @JsonProperty("first_value")
    String firstValue;
    @JsonProperty("second_value")
    String secondValue;

    public DataFrameBinaryKey(DataFrameColumnType columnType, String first, String second) {
        super(columnType);
        this.firstValue = first;
        this.secondValue = second;
    }
}
