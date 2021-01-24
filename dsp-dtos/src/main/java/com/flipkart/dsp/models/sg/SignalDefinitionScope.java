package com.flipkart.dsp.models.sg;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 */

@NoArgsConstructor
@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class SignalDefinitionScope {

    @JsonProperty("predicate_entity")
    private String predicateEntity;

    @JsonProperty("predicate_clause")
    private AbstractPredicateClause predicateClause;

}
