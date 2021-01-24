package com.flipkart.dsp.models.sg;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = UniValuePredicateClause.class, name = "uniValue"),
        @JsonSubTypes.Type(value = BiValuePredicateClause.class, name = "biValue"),
        @JsonSubTypes.Type(value = MultiValuePredicateClause.class, name = "multiValue")
})
@NoArgsConstructor
@Getter
@EqualsAndHashCode
@Data
public abstract class AbstractPredicateClause implements Serializable{

    @JsonProperty("predicate_type")
    private PredicateType predicateType;

    public AbstractPredicateClause(PredicateType predicateType) {
        this.predicateType = predicateType;
    }
}
