package com.flipkart.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DSPTestEnvVariables.class ,name="DSP")
})
@Setter
@Getter
public abstract class TestEnvVariables {
    public String emailId;
    public Boolean destroyEnv = false;
    public List<String> testScenarioList = null;
    public Boolean executeAllTestScenarios = true;
}
