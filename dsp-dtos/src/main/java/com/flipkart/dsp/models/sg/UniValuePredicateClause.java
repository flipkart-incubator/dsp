package com.flipkart.dsp.models.sg;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 */

@NoArgsConstructor
@Getter
@EqualsAndHashCode
public class UniValuePredicateClause extends AbstractPredicateClause implements Serializable{

    @JsonProperty("value")
    private Object value;

    public UniValuePredicateClause(PredicateType predicateType, Object value) {
        super(predicateType);
        this.value = value;
    }

}
