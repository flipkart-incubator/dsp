package com.flipkart.dsp.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TrainingWorkflow {

    @Valid
    @NotNull
    private String id;

    @Valid
    @NotNull
    @JsonProperty("group_id")
    private String groupId;
}
