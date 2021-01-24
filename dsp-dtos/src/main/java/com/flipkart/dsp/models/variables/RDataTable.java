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
public class RDataTable extends AbstractDataFrame implements Serializable {
    private String integer64;
    @JsonProperty
    private Boolean fill;

    public RDataTable() {
        setNaStrings(new ArrayList<String>(){{add("NA");add("");}});
        setEncoding("unknown");
        setQuoteCharacter("\"");
        this.integer64 = "integer64";
        this.fill = true;
    }

}
