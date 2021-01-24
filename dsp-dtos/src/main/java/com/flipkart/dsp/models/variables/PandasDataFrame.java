package com.flipkart.dsp.models.variables;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class PandasDataFrame extends AbstractDataFrame implements Serializable {

    public PandasDataFrame() {
        setNaStrings(new ArrayList<>());
        setEncoding("utf-8");
        setQuoteCharacter("\"");
    }
}
