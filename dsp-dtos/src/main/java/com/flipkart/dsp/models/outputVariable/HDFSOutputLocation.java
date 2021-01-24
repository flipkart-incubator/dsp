package com.flipkart.dsp.models.outputVariable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class HDFSOutputLocation extends OutputLocation {
    private String location;

    public boolean isSimilarOutputLocation(OutputLocation outputLocation) {
        if (outputLocation instanceof HDFSOutputLocation) {
            HDFSOutputLocation hdfsOutputLocation = (HDFSOutputLocation) outputLocation;
            return this.getLocation().equalsIgnoreCase(hdfsOutputLocation.getLocation());
        }
        return false;
    }
}
