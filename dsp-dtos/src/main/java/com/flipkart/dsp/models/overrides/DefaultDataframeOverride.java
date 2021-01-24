package com.flipkart.dsp.models.overrides;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DefaultDataframeOverride implements DataframeOverride {
    private Boolean forceRun;
}
