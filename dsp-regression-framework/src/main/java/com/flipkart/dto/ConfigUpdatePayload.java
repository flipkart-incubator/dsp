package com.flipkart.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ConfigUpdatePayload {
    @JsonProperty("azkabanConfig-jarVersion")
    private String azkabanJarVersion;

    @JsonProperty("miscConfig-executorJarVersion")
    private String executorJarVersion;
}
