package com.flipkart.dsp.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ScriptVariableOverride implements ObjectOverride {
    private String name;
    private DataType dataType;
    private Object value;
}
