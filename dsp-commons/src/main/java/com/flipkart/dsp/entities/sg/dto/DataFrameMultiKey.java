package com.flipkart.dsp.entities.sg.dto;


import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.io.Serializable;
import java.util.Set;

/**
 */

@NoArgsConstructor
@EqualsAndHashCode
@Getter
@ToString(callSuper = true)
@Setter
public class DataFrameMultiKey extends DataFrameKey implements Serializable{

    @JsonProperty("values")
    Set<String> values;

    public DataFrameMultiKey(DataFrameColumnType columnType, Set<String> values) {
        super(columnType);
        this.values = values;
    }
}
