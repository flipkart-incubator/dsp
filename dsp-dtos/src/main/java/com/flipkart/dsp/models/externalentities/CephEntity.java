package com.flipkart.dsp.models.externalentities;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * +
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CephEntity extends ExternalEntity implements Serializable {
    private String host;
    private String accessKey;
    private String secretKey;
}
