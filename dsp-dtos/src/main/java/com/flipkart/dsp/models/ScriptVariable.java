package com.flipkart.dsp.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.dsp.models.outputVariable.OutputLocation;
import lombok.*;

import java.util.List;
import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@ToString
@Builder
@JsonInclude(NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ScriptVariable {
    private String name;

    @JsonProperty("data_type")
    private DataType dataType;

    @Setter
    private Object value;

    @JsonProperty("additional_params")
    @Setter
    private AdditionalVariable additionalVariable;

    @Setter
    @JsonProperty(value = "output_location_details")
    private List<OutputLocation> outputLocationDetailsList;

    @JsonProperty
    private Boolean required = false;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScriptVariable that = (ScriptVariable) o;
        if (name.equals(that.name) && dataType.equals(that.dataType)) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, dataType);
    }
}
