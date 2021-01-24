package com.flipkart.dsp.dto;

import com.fasterxml.jackson.annotation.*;
import com.flipkart.dsp.models.sg.SignalDataType;
import com.flipkart.dsp.models.sg.SignalDefinition;
import io.dropwizard.jackson.JsonSnakeCase;
import lombok.*;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(NON_NULL)
@JsonSnakeCase
public class VisibilityDTO {

    @JsonProperty("data_frames")
    private Map<String, VisibilityDataFrame> dataFrameDetailsMap;

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonInclude(NON_NULL)
    public static class VisibilityDataFrame {
        @JsonProperty("signals")
        private LinkedHashMap<String, SignalDetails> signalDetailsMap;
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonInclude(NON_NULL)
    public static class SignalDetails {
        @JsonProperty("data_type")
        private SignalDataType signalDataType;
        @JsonProperty("definition")
        private SignalDefinition signalDefinition;
        @JsonProperty("is_primary")
        private boolean isPrimary;
        @JsonProperty
        private Source source;
    }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            property = "type"
    )
    public interface Source {

    }

}
