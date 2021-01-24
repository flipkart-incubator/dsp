package com.flipkart.dsp.sg.utils;

import com.flipkart.dsp.entities.sg.dto.*;
import com.flipkart.dsp.models.sg.BiValuePredicateClause;
import com.flipkart.dsp.models.sg.MultiValuePredicateClause;
import com.flipkart.dsp.models.sg.PredicateType;
import com.flipkart.dsp.models.sg.UniValuePredicateClause;
import com.flipkart.dsp.sg.hiveql.base.Column;
import com.flipkart.dsp.sg.hiveql.core.HiveTable;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Set;

import static com.flipkart.dsp.sg.utils.GeneratorUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * +
 */
public class GeneratorUtilsTest {
    private HiveTable hiveTable;
    @Mock private Column column;
    @Mock private BiValuePredicateClause biValuePredicateClause;
    @Mock private UniValuePredicateClause uniValuePredicateClause;
    @Mock private MultiValuePredicateClause multiValuePredicateClause;

    private Long dataFrameId = 1L;
    private String tableName = "test_table";
    private String firstSignal = "signal";
    private String dataFrameName = "dataFrameName";
    private Long dataFrameAuditRunId = 1L;

    private LinkedList<Column> columns = new LinkedList<>();
    private Set<Object> values = new HashSet<>();

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        columns.add(column);
        hiveTable = HiveTable.builder().columns(columns).build();
    }

    @Test
    public void testGetDataFrameTableName() {
        String actual = String.format(DATA_FRAME_TABLE_NAME, dataFrameName, dataFrameId, dataFrameAuditRunId);
        assertEquals(actual, GeneratorUtils.getDataFrameTableName(dataFrameAuditRunId, dataFrameName, dataFrameId));
    }

    @Test
    public void testGetGranularityTableName() {
        String actual = String.format(GRANULARITY_TABLE_NAME, dataFrameId, dataFrameAuditRunId);
        assertEquals(actual, GeneratorUtils.getGranularityTableName(dataFrameAuditRunId, dataFrameId.toString()));
    }

    @Test
    public void testGetFactTableName() {
        String actual = String.format(FACT_TABLE_NAME_IG, firstSignal, tableName, dataFrameId, dataFrameAuditRunId);
        assertEquals(actual, GeneratorUtils.getFactTableName(dataFrameAuditRunId, tableName, firstSignal, dataFrameId.toString()));
    }

    @Test
    public void testGetPartitionKeys() {
        when(column.isPartitionColumn()).thenReturn(true);
        LinkedHashSet<Column> expected = GeneratorUtils.getPartitionKeys(hiveTable);
        assertNotNull(expected);
        assertEquals(expected.size(), 1);
    }

    @Test
    public void testConvertAbstractPredicateClauseToTripletCase1() {
        when(multiValuePredicateClause.getPredicateType()).thenReturn(PredicateType.IN);
        when(multiValuePredicateClause.getValues()).thenReturn(values);

        Triplet<Object, Pair<Object, Object>, Set<Object>> expected = GeneratorUtils.convertAbstractPredicateClauseToTriplet(multiValuePredicateClause);
        assertNotNull(expected);
        assertEquals(expected.getSize(), 3);
        assertNull(expected.getValue0());
        assertNull(expected.getValue1());
        assertEquals(expected.getValue2(), values);
        verify(multiValuePredicateClause).getPredicateType();
        verify(multiValuePredicateClause).getValues();
    }

    @Test
    public void testConvertAbstractPredicateClauseToTripletCase2() {
        when(biValuePredicateClause.getPredicateType()).thenReturn(PredicateType.RANGE);
        when(biValuePredicateClause.getValue1()).thenReturn(values);
        when(biValuePredicateClause.getValue2()).thenReturn(values);

        Triplet<Object, Pair<Object, Object>, Set<Object>> expected = GeneratorUtils.convertAbstractPredicateClauseToTriplet(biValuePredicateClause);
        assertNotNull(expected);
        assertEquals(expected.getSize(), 3);
        assertNull(expected.getValue0());
        assertEquals(expected.getValue1(), Pair.with(values, values));
        assertNull(expected.getValue2());
        verify(biValuePredicateClause).getPredicateType();
        verify(biValuePredicateClause).getValue1();
        verify(biValuePredicateClause).getValue2();
    }

    @Test
    public void testConvertAbstractPredicateClauseToTripletCase3() {
        when(uniValuePredicateClause.getPredicateType()).thenReturn(PredicateType.EQUAL);
        when(uniValuePredicateClause.getValue()).thenReturn(values);

        Triplet<Object, Pair<Object, Object>, Set<Object>> expected = GeneratorUtils.convertAbstractPredicateClauseToTriplet(uniValuePredicateClause);
        assertNotNull(expected);
        assertEquals(expected.getSize(), 3);
        assertEquals(expected.getValue0(), values);
        assertNull(expected.getValue1());
        assertNull(expected.getValue2());
        verify(uniValuePredicateClause).getPredicateType();
        verify(uniValuePredicateClause).getValue();
    }

    @Test
    public void testConvertAbstractPredicateClauseToTripletCase4() {
        when(uniValuePredicateClause.getPredicateType()).thenReturn(PredicateType.GREATER_THAN);
        when(uniValuePredicateClause.getValue()).thenReturn(values);

        Triplet<Object, Pair<Object, Object>, Set<Object>> expected = GeneratorUtils.convertAbstractPredicateClauseToTriplet(uniValuePredicateClause);
        assertNotNull(expected);
        assertEquals(expected.getSize(), 3);
        assertEquals(expected.getValue0(), values);
        assertNull(expected.getValue1());
        assertNull(expected.getValue2());
        verify(uniValuePredicateClause).getPredicateType();
        verify(uniValuePredicateClause).getValue();
    }

    @Test
    public void testConvertAbstractPredicateClauseToTripletCase5() {
        when(uniValuePredicateClause.getPredicateType()).thenReturn(PredicateType.LESS_THAN);
        when(uniValuePredicateClause.getValue()).thenReturn(values);

        Triplet<Object, Pair<Object, Object>, Set<Object>> expected = GeneratorUtils.convertAbstractPredicateClauseToTriplet(uniValuePredicateClause);
        assertNotNull(expected);
        assertEquals(expected.getSize(), 3);
        assertEquals(expected.getValue0(), values);
        assertNull(expected.getValue1());
        assertNull(expected.getValue2());
        verify(uniValuePredicateClause).getPredicateType();
        verify(uniValuePredicateClause).getValue();
    }

    @Test
    public void testConvertAbstractPredicateClauseToDataFrameKeyCase1() {
        when(uniValuePredicateClause.getPredicateType()).thenReturn(PredicateType.EQUAL);
        when(uniValuePredicateClause.getValue()).thenReturn(values);

        DataFrameKey dataFrameKey = GeneratorUtils.convertAbstractPredicateClauseToDataFrameKey(uniValuePredicateClause);
        assertEquals(dataFrameKey.getColumnType(),  DataFrameColumnType.IN);
        assertTrue(dataFrameKey instanceof DataFrameMultiKey);
        verify(uniValuePredicateClause).getPredicateType();
        verify(uniValuePredicateClause).getValue();
    }

    @Test
    public void testConvertAbstractPredicateClauseToDataFrameKeyCase2() {
        when(multiValuePredicateClause.getPredicateType()).thenReturn(PredicateType.IN);
        when(multiValuePredicateClause.getValues()).thenReturn(values);

        DataFrameKey dataFrameKey = GeneratorUtils.convertAbstractPredicateClauseToDataFrameKey(multiValuePredicateClause);
        assertEquals(dataFrameKey.getColumnType(),  DataFrameColumnType.IN);
        assertTrue(dataFrameKey instanceof DataFrameMultiKey);
        verify(multiValuePredicateClause).getPredicateType();
        verify(multiValuePredicateClause).getValues();
    }

    @Test
    public void testConvertAbstractPredicateClauseToDataFrameKeyCase3() {
        when(biValuePredicateClause.getPredicateType()).thenReturn(PredicateType.INCREMENTAL_DATE_RANGE);
        when(biValuePredicateClause.getValue1()).thenReturn(values);
        when(biValuePredicateClause.getValue2()).thenReturn(values);

        DataFrameKey dataFrameKey = GeneratorUtils.convertAbstractPredicateClauseToDataFrameKey(biValuePredicateClause);
        assertEquals(dataFrameKey.getColumnType(),  DataFrameColumnType.RANGE);
        assertTrue(dataFrameKey instanceof DataFrameBinaryKey);
        verify(biValuePredicateClause).getPredicateType();
        verify(biValuePredicateClause).getValue1();
        verify(biValuePredicateClause).getValue2();
    }

    @Test
    public void testConvertAbstractPredicateClauseToDataFrameKeyCase4() {
        when(uniValuePredicateClause.getPredicateType()).thenReturn(PredicateType.LESS_THAN);
        when(uniValuePredicateClause.getValue()).thenReturn(values);

        DataFrameKey dataFrameKey = GeneratorUtils.convertAbstractPredicateClauseToDataFrameKey(uniValuePredicateClause);
        assertEquals(dataFrameKey.getColumnType(),  DataFrameColumnType.EQUAL);
        assertTrue(dataFrameKey instanceof DataFrameUnaryKey);
        verify(uniValuePredicateClause).getPredicateType();
        verify(uniValuePredicateClause).getValue();
    }

    @Test
    public void testConvertAbstractPredicateClauseToDataFrameKeyFailure() {
        when(uniValuePredicateClause.getPredicateType()).thenReturn(PredicateType.NOT_IN);

        boolean isException = false;
        try {
            GeneratorUtils.convertAbstractPredicateClauseToDataFrameKey(uniValuePredicateClause);
        } catch (RuntimeException e) {
            isException = true;
            String message = String.format("Failed to convert AbstractPredicateClause of type %s to DataFrameKey.", PredicateType.NOT_IN);
            assertEquals(e.getMessage(), message);
        }

        assertTrue(isException);
        verify(uniValuePredicateClause, times(2)).getPredicateType();
    }
}
