package com.flipkart.dsp.models.outputVariable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.dsp.models.sg.SignalDataType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.LinkedHashMap;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class CephOutputLocation extends OutputLocation implements Serializable {
    private String path;
    private String bucket;
    private String clientAlias;
    private LinkedHashMap<String, SignalDataType> columnMapping;
    private boolean merged = true;

    public boolean isSimilarOutputLocation(OutputLocation outputLocation) {
        if (outputLocation instanceof CephOutputLocation) {
            CephOutputLocation cephOutputLocation = (CephOutputLocation) outputLocation;
            return this.getBucket().equalsIgnoreCase(cephOutputLocation.getBucket())
                    && this.getPath().equalsIgnoreCase(cephOutputLocation.getPath())
                    && this.getClientAlias().equalsIgnoreCase(cephOutputLocation.getClientAlias())
                    && isSimilarOutputColumnMapping(this.columnMapping, cephOutputLocation.getColumnMapping());
        }
        return false;
    }
}
