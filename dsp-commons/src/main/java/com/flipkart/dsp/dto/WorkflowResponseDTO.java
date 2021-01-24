package com.flipkart.dsp.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.Resources;
import com.flipkart.dsp.models.outputVariable.OutputLocation;
import com.flipkart.dsp.models.sg.SignalDataType;
import io.dropwizard.jackson.JsonSnakeCase;
import lombok.*;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(NON_NULL)
@JsonSnakeCase
public class WorkflowResponseDTO implements Serializable {
    private String version;
    private Boolean isProd;
    private String workflowName;
    private List<Steps> steps;

    @NoArgsConstructor
    @EqualsAndHashCode
    @Getter
    @Setter
    @Builder
    @JsonSnakeCase
    @AllArgsConstructor
    public static class Steps {
        private String name;
        private Script script;
        private Resources resources;
        private Map<String, InputParameter> inputs;
        private Map<String, List<OutputLocation>> outputs;
        private Set<String> partitions;
    }


    @NoArgsConstructor
    @EqualsAndHashCode
    @Getter
    @Setter
    @Builder
    @AllArgsConstructor
    @JsonSnakeCase
    public static class Script {
        private String executionEnv;
        private String gitRepo;
        private String gitLocation;
        private String gitCommitId;
    }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            property = "type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = Variables.class, name = "VARIABLE"),
            @JsonSubTypes.Type(value = InputDataFrame.class, name = "DATAFRAME")
    })
    public interface InputParameter extends Serializable {

    }

    @NoArgsConstructor
    @EqualsAndHashCode
    @Getter
    @ToString(callSuper = true)
    @Setter
    @AllArgsConstructor
    @JsonSnakeCase
    public static class Variables implements InputParameter, Serializable {
        private DataType dataType;
        private Object value;
    }

    @NoArgsConstructor
    @Getter
    @Setter
    @AllArgsConstructor
    @Data
    @ToString(callSuper = true)
    @JsonSnakeCase
    public static class InputDataFrame implements InputParameter, Serializable {
        private Long dataframeId;
        private LinkedHashMap<String, ColumnDetails> columnDetails;

    }

    @NoArgsConstructor
    @EqualsAndHashCode
    @ToString
    @Getter
    @AllArgsConstructor
    @JsonSnakeCase
    public static class ColumnDetails {
        private SignalDataType type;
        private String name;
    }

}
