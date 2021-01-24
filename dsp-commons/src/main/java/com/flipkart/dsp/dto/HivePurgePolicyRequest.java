package com.flipkart.dsp.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.dsp.entities.enums.Level;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HivePurgePolicyRequest {
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
        private final String dataStore = "HIVE";
        @JsonProperty("databaseName")
        public String databaseName;
        @JsonProperty("tableName")
        public String tableName;
        @JsonProperty("partitionColumn")
        public String partitionColumn;
        @JsonProperty("level")
        private Level level;
    }
}
