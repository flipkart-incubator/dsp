package com.flipkart.dsp.models.variables;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class RDataFrame extends AbstractDataFrame implements Serializable {

    @JsonProperty
    private Boolean fill;

    public RDataFrame() {
        setNaStrings(new ArrayList<String>(){{add("NA");add("");}});
        setEncoding("unknown");
        setQuoteCharacter("\"");
        this.fill = false;

    }

}
