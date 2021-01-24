package com.flipkart.dsp.entities.misc;

import com.flipkart.dsp.models.sg.PredicateType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * +
 */
@Data
@Builder
@AllArgsConstructor
public class SqlParam {
    private String paramName;
    private Object paramValues;
    private PredicateType predicateType;
}
