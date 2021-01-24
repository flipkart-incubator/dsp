package com.flipkart.dsp.sg.hiveql.core;

import com.flipkart.dsp.models.sg.PredicateType;
import com.flipkart.dsp.sg.hiveql.base.Column;
import com.flipkart.dsp.sg.hiveql.base.Constraint;
import com.flipkart.dsp.sg.hiveql.query.HiveQueryConstants;
import com.flipkart.dsp.utils.Constants;
import org.javatuples.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashSet;
import java.util.Set;

import static com.flipkart.dsp.sg.hiveql.query.HiveQueryConstants.*;
import static com.flipkart.dsp.sg.hiveql.query.SelectQuery.UNIX_TIMESTAMP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ConstraintType.class})
public class ConstraintTypeTest {

    @Mock private Column column;
    @Mock private Constraint constraint;

    private Set<Object> inValues = new HashSet<>();
    private Pair<Object, Object> range = new Pair<>(0, 1);

    @Before
    public void setUp() {
        PowerMockito.spy(ConstraintType.class);
        MockitoAnnotations.initMocks(this);

        String columnName = "column1";
        inValues.add("value1");
        inValues.add("value2");

        when(constraint.getRange()).thenReturn(range);
        when(constraint.getInValues()).thenReturn(inValues);
        when(constraint.getConstraintColumn()).thenReturn(column);
        when(constraint.getConstraintValue()).thenReturn("value");
        when(constraint.getConstraintType()).thenReturn(ConstraintType.IN);

        when(column.getColumnName()).thenReturn(columnName);
        when(column.getColumnDataType()).thenReturn(ColumnDataType.INT);
    }

    @Test
    public void testFromSuccess() {
        ConstraintType[] constraintTypes = new ConstraintType[1];
        constraintTypes[0] = ConstraintType.IN;
        PowerMockito.when(ConstraintType.values()).thenReturn(constraintTypes);
        assertEquals(ConstraintType.from(PredicateType.IN), ConstraintType.IN);
    }

    @Test
    public void testFromFailure() {
        ConstraintType[] constraintTypes = new ConstraintType[1];
        constraintTypes[0] = ConstraintType.EQUAL;
        PowerMockito.when(ConstraintType.values()).thenReturn(constraintTypes);

        boolean isException = false;
        try {
            ConstraintType.from(PredicateType.IN);
        } catch (RuntimeException e) {
            isException = true;
            assertEquals(e.getMessage(), "Failed to map SignalDefinitionScopeType to ConstraintType !! Predicate type : " + PredicateType.IN);
        }

        assertTrue(isException);
    }

    @Test
    public void testGetConstraints() {
        String expected = ConstraintType.getConstraints("table_name", constraint);
        assertEquals(expected, " table_name.column1 IN (value2,value1)");
        verify(constraint, times(3)).getConstraintType();
        verify(constraint, times(3)).getConstraintColumn();
        verify(column).getColumnName();
        verify(constraint).getInValues();
    }

    // PredictiveType = IN
    @Test
    public void testIn() {
        when(column.getColumnDataType()).thenReturn(ColumnDataType.STRING);
        assertEquals(ConstraintType.IN.getPrefix(), SINGLE_SPACE);
        assertEquals(ConstraintType.IN.getPostfix(), HiveQueryConstants.IN);
        assertEquals(ConstraintType.IN.addConstraintValue(constraint), "('value2','value1')");
        verify(constraint).getInValues();
        verify(constraint, times(2)).getConstraintColumn();
        verify(column, times(2)).getColumnDataType();
    }

    // PredictiveType = NOT_IN
    @Test
    public void testNotIn() {
        when(column.getColumnDataType()).thenReturn(ColumnDataType.STRING);
        assertEquals(ConstraintType.NOT_IN.getPrefix(), SINGLE_SPACE);
        assertEquals(ConstraintType.NOT_IN.getPostfix(), HiveQueryConstants.NOT_IN);
        assertEquals(ConstraintType.NOT_IN.addConstraintValue(constraint), "('value2','value1')");
        verify(constraint).getInValues();
        verify(constraint, times(2)).getConstraintColumn();
        verify(column, times(2)).getColumnDataType();
    }

    // PredictiveType = EQUAL
    @Test
    public void testNotEqual() {
        assertEquals(ConstraintType.EQUAL.getPrefix(), "");
        assertEquals(ConstraintType.EQUAL.getPostfix(), Constants.equal);
        assertEquals(ConstraintType.EQUAL.addConstraintValue(constraint), "value");
        verify(constraint).getConstraintValue();
        verify(constraint, times(1)).getConstraintColumn();
        verify(column, times(1)).getColumnDataType();
    }

    // PredictiveType = RANGE
    @Test
    public void testRange() {
        assertEquals(ConstraintType.RANGE.getPrefix(), "");
        assertEquals(ConstraintType.RANGE.getPostfix(), BETWEEN);
        assertEquals(ConstraintType.RANGE.addConstraintValue(constraint), "0 AND 1");
        verify(constraint, times(2)).getRange();
    }

    // PredictiveType = LESS_THAN
    @Test
    public void testLessThan() {
        assertEquals(ConstraintType.LESS_THAN.getPrefix(), "");
        assertEquals(ConstraintType.LESS_THAN.getPostfix(), "<");
        assertEquals(ConstraintType.LESS_THAN.addConstraintValue(constraint), "value");
        verify(constraint).getConstraintValue();
    }

    // PredictiveType = GREATER_THAN
    @Test
    public void testGreaterTham() {
        assertEquals(ConstraintType.GREATER_THAN.getPrefix(), "");
        assertEquals(ConstraintType.GREATER_THAN.getPostfix(), ">");
        assertEquals(ConstraintType.GREATER_THAN.addConstraintValue(constraint), "value");
        verify(constraint).getConstraintValue();
    }

    // PredictiveType = TIME_YYYYWW_RANGE
    @Test
    public void testTimeRange() {
        assertEquals(ConstraintType.TIME_YYYYWW_RANGE.getPrefix(), "");
        assertEquals(ConstraintType.TIME_YYYYWW_RANGE.getPostfix(), BETWEEN);
        assertEquals(ConstraintType.TIME_YYYYWW_RANGE.addConstraintValue(constraint), "0 AND 1");
        verify(constraint, times(2)).getRange();
    }

    // PredictiveType = TIME_YYYYMMDD_RANGE
    @Test
    public void testTimeYearRange() {
        assertEquals(ConstraintType.TIME_YYYYMMDD_RANGE.getPrefix(),UNIX_TIMESTAMP + OPEN_BRACKET);
        assertEquals(ConstraintType.TIME_YYYYMMDD_RANGE.getPostfix(),  COMMA + YYYY_MM_DD + CLOSE_BRACKET);
        assertEquals(ConstraintType.TIME_YYYYMMDD_RANGE.addConstraintValue(constraint), "BETWEEN UNIX_TIMESTAMP('0','yyyy-MM-dd') AND UNIX_TIMESTAMP('1','yyyy-MM-dd')");
        verify(constraint, times(2)).getRange();
    }

    // PredictiveType = INCREMENTAL_DATE_RANGE
    @Test
    public void testIncrementalDateRange() {
        assertEquals(ConstraintType.INCREMENTAL_DATE_RANGE.getPrefix(),UNIX_TIMESTAMP + OPEN_BRACKET);
        assertEquals(ConstraintType.INCREMENTAL_DATE_RANGE.getPostfix(),  SINGLE_SPACE);
        assertEquals(ConstraintType.INCREMENTAL_DATE_RANGE.addConstraintValue(constraint), "BETWEEN UNIX_TIMESTAMP('0','yyyy-MM-dd') AND UNIX_TIMESTAMP('1','yyyy-MM-dd')");
        verify(constraint, times(2)).getRange();
    }

    // PredictiveType = INCREMENTAL_WEEK_RANGE
    @Test
    public void testIncrementalWeekRange() {
        assertEquals(ConstraintType.INCREMENTAL_WEEK_RANGE.getPrefix(), "");
        assertEquals(ConstraintType.INCREMENTAL_WEEK_RANGE.getPostfix(), BETWEEN);
        assertEquals(ConstraintType.INCREMENTAL_WEEK_RANGE.addConstraintValue(constraint), "0 AND 1");
        verify(constraint, times(2)).getRange();
    }

    @Test
    public void testAddTableName() {
        assertEquals(ConstraintType.IN.addTableName("test_table"), "test_table.");
    }

}
