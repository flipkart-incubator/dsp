package com.flipkart.dsp.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HdfsPurgePolicyRequest {
    @JsonProperty("config")
    private Config config;
    @JsonProperty("purge_policy")
    private PurgePolicyRequest purgePolicyRequest;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Config {
        @JsonProperty("dataStore")
        private final String dataStore = "HDFS";
        @JsonProperty("path")
        public String path;
        @JsonProperty("subPathPrefix")
        public String subPathPrefix;
    }
}
