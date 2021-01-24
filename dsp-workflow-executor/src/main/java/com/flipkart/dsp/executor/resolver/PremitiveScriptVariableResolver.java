package com.flipkart.dsp.executor.resolver;

import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.ScriptVariableOverride;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

@AllArgsConstructor
@Data
public class PremitiveScriptVariableResolver {
    public ScriptVariable resolve(Map<String, ScriptVariableOverride> scriptVariableOverrideMap, ScriptVariable scriptVariable) {
        final String name = scriptVariable.getName();
        if(scriptVariableOverrideMap.containsKey(name)) {
            final ScriptVariableOverride scriptVariableOverride = scriptVariableOverrideMap.get(name);
            if (!scriptVariableOverride.getDataType().equals(scriptVariable.getDataType())) {
                throw new RuntimeException("Invalid Script Override: " + scriptVariableOverride.getName()
                        + " DataType Expected: " + scriptVariable.getName() + " DataType Received: "
                        + scriptVariableOverride.getDataType());
            }
            scriptVariable.setValue(scriptVariableOverride.getValue());
        }
        return scriptVariable;
    }
}
