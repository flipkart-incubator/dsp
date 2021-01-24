package com.flipkart.dsp.sg.hiveql.core;

import com.flipkart.dsp.sg.hiveql.base.Column;
import com.flipkart.dsp.sg.hiveql.base.Constraint;
import com.flipkart.dsp.sg.hiveql.base.Table;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * +
 */
public class HiveJoinTest {
    @Mock private Table table;
    @Mock private Column column;
    @Mock private Constraint constraint;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void test() {
        Set<Constraint> constraints = new HashSet<>();
        Map<Column, Column> joinColumns = new HashMap<>();
        HiveJoin hiveJoin = HiveJoin.builder().leftTable(table).rightTable(table).joinClause(constraints).joinColumns(joinColumns).build();

        assertEquals(hiveJoin.getLeftTable(), table);
        assertEquals(hiveJoin.getRightTable(), table);
        assertEquals(hiveJoin.getJoinColumns(), joinColumns);
        assertEquals(hiveJoin.getJoinClause(), constraints);
    }
}
