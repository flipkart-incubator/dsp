package com.flipkart.dsp.models;

import lombok.*;

/**
 * +
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ExternalCredentials {
    private String clientAlias;
    private String details;
}
