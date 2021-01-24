package com.flipkart.dsp.models.sg;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 */
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Getter
@JsonAutoDetect
public class DataSource implements Serializable{

    @JsonProperty("id")
    private String id;

    @JsonProperty("configuration")
    private DataSourceConfiguration configuration;
}
