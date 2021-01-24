package com.flipkart.dsp.models.outputVariable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.dsp.models.sg.SignalDataType;

import java.util.LinkedHashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = CephOutputLocation.class, name = "CephOutputLocation"),
        @JsonSubTypes.Type(value = HiveOutputLocation.class, name = "HiveOutputLocation"),
        @JsonSubTypes.Type(value = HDFSOutputLocation.class, name = "HDFSOutputLocation"),
})
public abstract class OutputLocation {

    @JsonIgnore
    public abstract boolean isSimilarOutputLocation(OutputLocation outputLocation);

    @JsonIgnore
    protected boolean isSimilarOutputColumnMapping(LinkedHashMap<String, SignalDataType> currentColumnMapping,
                                                   LinkedHashMap<String, SignalDataType> existingColumnMapping) {
        if (currentColumnMapping == null && existingColumnMapping == null)
            return true;
        else if (currentColumnMapping != null && existingColumnMapping != null) {
            return currentColumnMapping.size() == existingColumnMapping.size()
                    && currentColumnMapping.entrySet().stream().noneMatch(currentColumn -> existingColumnMapping.entrySet().stream()
                    .noneMatch(existingColumn -> currentColumn.getKey().equalsIgnoreCase(existingColumn.getKey())
                            && currentColumn.getValue().equals(existingColumn.getValue())));
        }
        return false;
    }
}
