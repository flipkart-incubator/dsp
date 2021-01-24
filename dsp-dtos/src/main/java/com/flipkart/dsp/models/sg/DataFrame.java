package com.flipkart.dsp.models.sg;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@JsonAutoDetect
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataFrame implements Serializable {

    private Long id;

    private String name;

    private SignalGroup signalGroup;

    @JsonProperty("config")
    private DataFrameConfig dataFrameConfig;

    private SGType sgType;

    private String tableName;
    private List<String> partitions;

    @JsonIgnore
    public SGType getSgType() {
        return sgType;
    }

    public void setSgType(SGType sgType) {
        this.sgType = sgType;
    }
}
