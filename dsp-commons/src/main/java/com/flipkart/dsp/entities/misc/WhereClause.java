package com.flipkart.dsp.entities.misc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown=true)
@JsonAutoDetect
@AllArgsConstructor
@NoArgsConstructor
@Data
@JsonPropertyOrder(alphabetic=true)
public class WhereClause implements Serializable {

    public enum WhereType{
        IN, RANGE,
    }

    @JsonProperty("id")
    private String id ;

    @JsonProperty(value = "type", required = true)
    private WhereType whereType;

    @JsonProperty("values_in")
    private Set<String> values;

    @JsonProperty("range_start")
    private String rangeStart;

    @JsonProperty("range_end")
    private String rangeEnd;
}
