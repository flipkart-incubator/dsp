package com.flipkart.dsp.sg.hiveql.core;

import com.flipkart.dsp.sg.hiveql.base.Column;
import com.flipkart.dsp.sg.hiveql.base.Constraint;
import com.flipkart.dsp.sg.hiveql.base.Join;
import com.flipkart.dsp.sg.hiveql.base.Table;
import lombok.Builder;
import lombok.RequiredArgsConstructor;

import java.util.Map;
import java.util.Set;

/**
 */

@Builder
@RequiredArgsConstructor
public class HiveJoin implements Join {
    private final Table leftTable;
    private final Table rightTable;
    private final Map<Column, Column> joinColumns;
    private final Set<Constraint> joinClause;

    @Override
    public Table getLeftTable() {
        return leftTable;
    }

    @Override
    public Table getRightTable() {
        return rightTable;
    }

    @Override
    public Map<Column, Column> getJoinColumns() {
        return joinColumns;
    }

    @Override
    public Set<Constraint> getJoinClause() {
        return joinClause;
    }
}
