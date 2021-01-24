package com.flipkart.dsp.models.overrides;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RunIdDataframeOverride implements DataframeOverride {
    private Long runId;
}
