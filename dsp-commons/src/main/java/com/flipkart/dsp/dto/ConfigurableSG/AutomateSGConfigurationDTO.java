package com.flipkart.dsp.dto.ConfigurableSG;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.dsp.models.sg.SignalDataType;
import lombok.*;

import javax.persistence.Entity;

@Data
@Entity
@NoArgsConstructor
@JsonAutoDetect
@Builder
@AllArgsConstructor
public class AutomateSGConfigurationDTO {

    public SignalDetail signalDetail;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SignalDetail {
        private String signalId;
        private String signalBaseEntity;
        private Boolean isPrimary;
        private Boolean isVisible;
        private SignalDataType signalDataType;
        private String dbName;
        private String tableName;

    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ColumnDetails {
        @JsonProperty("name")
        private String name;
        @JsonProperty("type")
        private String type;
        @JsonProperty("primaryKey")
        private boolean primaryKey = false;
        @JsonProperty("partition")
        private boolean partition = false;
    }

    public enum SGType {
        CSV,
        HIVE_QUERY,
        HIVE_TABLE
    }
}
