package com.flipkart.dsp.models.sg;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.io.Serializable;

/**
 */

@Getter
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@Data
public class DataFrameScope implements Serializable{
    @JsonProperty("predicate_entity")
    private Signal signal;

    @JsonProperty("predicate")
    private AbstractPredicateClause abstractPredicateClause;

}
