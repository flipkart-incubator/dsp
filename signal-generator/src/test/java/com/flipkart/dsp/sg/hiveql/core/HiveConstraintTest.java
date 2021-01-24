package com.flipkart.dsp.sg.hiveql.core;

import com.flipkart.dsp.sg.hiveql.base.Column;
import org.javatuples.Pair;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * +
 */
public class HiveConstraintTest {
    @Mock private Column column;
    @Mock private Object constraintValue;
    @Mock private ConstraintType constraintType;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void test() {
        String tableName = "tableName";
        Set<Object> inValues = new HashSet<>();
        Pair<Object, Object> range = new Pair<>(1, 1);

        HiveConstraint hiveConstraint = HiveConstraint.builder().constraintColumn(column).tableName(tableName).rangeValues(range)
                .inValues(inValues).constraintValue(constraintValue).constraintType(constraintType).build();

        assertEquals(hiveConstraint.getRange(), range);
        assertEquals(hiveConstraint.getInValues(), inValues);
        assertEquals(hiveConstraint.getTableName(), tableName);
        assertEquals(hiveConstraint.getConstraintColumn(), column);
        assertEquals(hiveConstraint.getConstraintType(), constraintType);
        assertEquals(hiveConstraint.getConstraintValue(), constraintValue);
    }
}
