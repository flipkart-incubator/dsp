package com.flipkart.dsp.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.jackson.JsonSnakeCase;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(NON_NULL)
@JsonSnakeCase
public class RunDetailsDTO {
    @JsonProperty("runs")
    Map<Long, LocationDetails> runIDMap;

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonInclude(NON_NULL)
    public static class LocationDetails {
        @JsonProperty
        private String dataBaseName;
        @JsonProperty
        private String tableName;
        @JsonProperty
        private String refreshId;
    }
}

