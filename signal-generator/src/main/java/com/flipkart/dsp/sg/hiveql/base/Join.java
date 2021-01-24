package com.flipkart.dsp.sg.hiveql.base;

import com.flipkart.dsp.sg.hiveql.core.HiveColumn;
import com.flipkart.dsp.sg.hiveql.core.HiveConstraint;

import java.util.Map;
import java.util.Set;

/**
 */
public interface Join {

    public Table getLeftTable();

    public Table getRightTable();

    public Map<Column, Column> getJoinColumns();

    public Set<Constraint> getJoinClause();
}
