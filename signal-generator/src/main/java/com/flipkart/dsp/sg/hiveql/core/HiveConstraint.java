package com.flipkart.dsp.sg.hiveql.core;

import com.flipkart.dsp.sg.hiveql.base.Column;
import com.flipkart.dsp.sg.hiveql.base.Constraint;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import org.javatuples.Pair;

import java.util.Set;

/**
 */

@Builder
@RequiredArgsConstructor
public class HiveConstraint implements Constraint {

    private final Column constraintColumn;

    private final ConstraintType constraintType;

    private final Object constraintValue;

    private final Pair<Object, Object> rangeValues;

    private final Set<Object> inValues;

    private final String tableName;

    @Override
    public Column getConstraintColumn() {
        return constraintColumn;
    }

    @Override
    public ConstraintType getConstraintType() {
        return constraintType;
    }

    @Override
    public Object getConstraintValue() {
        return constraintValue;
    }

    @Override
    public Pair<Object, Object> getRange() {
        return rangeValues;
    }

    @Override
    public Set<Object> getInValues() {
        return inValues;
    }

    @Override
    public String getTableName() {
        return tableName;
    }
}
