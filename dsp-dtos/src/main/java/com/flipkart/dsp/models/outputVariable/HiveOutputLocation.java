package com.flipkart.dsp.models.outputVariable;

import com.flipkart.dsp.models.sg.SignalDataType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.LinkedHashMap;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class HiveOutputLocation extends OutputLocation implements Serializable {
    private String database;
    private String table;
    private LinkedHashMap<String, SignalDataType> columnMapping;
    private Boolean forceDelete = false;

    public boolean isSimilarOutputLocation(OutputLocation outputLocation) {
        if (outputLocation instanceof HiveOutputLocation) {
            HiveOutputLocation hiveOutputLocation = (HiveOutputLocation) outputLocation;
            return this.getDatabase().equalsIgnoreCase(hiveOutputLocation.getDatabase())
                    && this.getTable().equalsIgnoreCase(hiveOutputLocation.getTable())
                    && isSimilarOutputColumnMapping(this.columnMapping, hiveOutputLocation.getColumnMapping());
        }
        return false;
    }
}
