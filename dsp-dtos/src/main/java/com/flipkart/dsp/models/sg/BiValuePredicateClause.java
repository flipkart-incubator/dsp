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
public class BiValuePredicateClause extends AbstractPredicateClause implements Serializable{

    @JsonProperty("value1")
    Object value1;

    @JsonProperty("value2")
    Object value2;

    public BiValuePredicateClause(PredicateType predicateType, Object value1, Object value2) {
        super(predicateType);
        this.value1 = value1;
        this.value2 = value2;
    }

}
