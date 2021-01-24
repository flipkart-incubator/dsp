package com.flipkart.dsp.models.sg;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Set;

/**
 */

@NoArgsConstructor
@Getter
@EqualsAndHashCode
public class MultiValuePredicateClause extends AbstractPredicateClause implements Serializable{

    @JsonProperty("values")
    private Set<Object> values;

    public MultiValuePredicateClause(PredicateType predicateType, Set<Object> values) {
        super(predicateType);
        this.values = values;
    }
}
