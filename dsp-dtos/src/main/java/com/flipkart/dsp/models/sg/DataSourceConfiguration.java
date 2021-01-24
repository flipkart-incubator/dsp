package com.flipkart.dsp.models.sg;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonAutoDetect
public class DataSourceConfiguration implements Serializable{

    @JsonProperty("type")
    private DataSourceConfigurationType type;

    @JsonProperty("host_ip")
    private String host;

    @JsonProperty("database")
    private String database;
}
