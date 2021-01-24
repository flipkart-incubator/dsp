package com.flipkart.dsp.models;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "inputType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = RunStatusDTO.ModelGroupVariable.class, name = "MODEL"),
        @JsonSubTypes.Type(value = RunStatusDTO.DataFrameVariable.class, name = "HIVE_TABLE")
})
@NoArgsConstructor
@EqualsAndHashCode
@ToString
@Getter
public abstract class Variable  implements Serializable {
}