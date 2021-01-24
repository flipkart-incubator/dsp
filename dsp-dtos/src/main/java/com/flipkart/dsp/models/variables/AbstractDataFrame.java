package com.flipkart.dsp.models.variables;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.flipkart.dsp.models.AdditionalVariable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@Setter
@JsonSubTypes({
        @JsonSubTypes.Type(value = PandasDataFrame.class, name = "PANDAS_DATAFRAME"),
        @JsonSubTypes.Type(value = RDataFrame.class, name = "R_DATAFRAME"),
        @JsonSubTypes.Type(value = RDataTable.class, name = "R_DATATABLE")
})
public abstract class AbstractDataFrame extends AdditionalVariable {
    @JsonProperty("hive_table")
    private String hiveTable;

    private String separator = ",";

    @JsonProperty
    private String headers;

    @JsonProperty
    private String headerDataTypes;

    @JsonProperty("with_header")
    private Boolean withHeader = false;

    @JsonProperty("na_strings")
    private List<String> naStrings;

    @JsonProperty("encoding")
    private String encoding;

    @JsonProperty("quote_char")
    private String quoteCharacter;
}
