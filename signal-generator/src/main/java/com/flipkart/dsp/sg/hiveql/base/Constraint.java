package com.flipkart.dsp.sg.hiveql.base;

import com.flipkart.dsp.sg.hiveql.core.ConstraintType;
import org.javatuples.Pair;

import java.util.Set;

/**
 */
public interface Constraint {
    public Column getConstraintColumn();

    public ConstraintType getConstraintType();

    public Object getConstraintValue();

    public Pair<Object, Object> getRange();

    public Set<Object> getInValues();

    public String getTableName();

}
