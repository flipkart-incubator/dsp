package com.flipkart.dsp.dto;

import io.dropwizard.jackson.JsonSnakeCase;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonSnakeCase
public class NameNodeResponse {
    private String activeNN;
    private String cluster;
}
