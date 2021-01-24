package com.flipkart.dsp.models.sg;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class DataFrameConfig implements Serializable{
    @JsonProperty("dataframe_scope")
    Set<DataFrameScope> dataFrameScopeSet;

    @JsonProperty("signals")
    @JsonAlias({"visible_signals"})
    LinkedHashSet<Signal> visibleSignals;
}
