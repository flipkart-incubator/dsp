package com.flipkart.dsp.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.dropwizard.jackson.JsonSnakeCase;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonSnakeCase
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PurgePolicyRequest {
    private Integer versions;
    private String size;
    private String time;
}
