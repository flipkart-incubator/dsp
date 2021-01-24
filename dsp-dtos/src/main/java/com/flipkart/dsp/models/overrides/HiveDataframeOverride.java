package com.flipkart.dsp.models.overrides;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class HiveDataframeOverride implements DataframeOverride {
    private String database;
    private String tableName;
    private Long refreshId;
    private Boolean isIntermediate;
}
