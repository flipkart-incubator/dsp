package com.flipkart.dsp.sg.hiveql.query;

import com.flipkart.dsp.models.sg.AggregationType;
import com.flipkart.dsp.sg.hiveql.base.Column;
import com.flipkart.dsp.sg.hiveql.base.Constraint;
import com.flipkart.dsp.sg.hiveql.base.Join;
import com.flipkart.dsp.sg.hiveql.base.Table;
import com.flipkart.dsp.sg.hiveql.core.ColumnDataType;
import com.flipkart.dsp.sg.hiveql.core.ConstraintType;
import com.flipkart.dsp.sg.hiveql.core.HiveTable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SelectQuery.class, ConstraintType.class, ColumnDataType.class})
public class SelectQueryTest {
    @Mock private Join join;
    @Mock private Column column1;
    @Mock private Column column2;
    @Mock private Constraint constraint;
    @Mock private SelectQuery subQuery;
    @Mock private SelectColumn selectColumn1;
    @Mock private SelectColumn selectColumn2;

    private SelectQuery selectQuery;
    private String columnName2 = "*";
    private String columnName1 = "column1";
    private String columnName3 = "column3";
    private String tableName = "test_table";
    private Set<Join> joins = new HashSet<>();
    private Set<String> groupByColumns = new HashSet<>();
    private Set<Constraint> constraints = new HashSet<>();
    private LinkedList<Column> columns = new LinkedList<>();
    private Map<Column, Column> columnMap = new HashMap<>();
    private LinkedHashSet<Table> tables = new LinkedHashSet<>();
    private LinkedHashSet<SelectColumn> selectColumns = new LinkedHashSet<>();

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(ConstraintType.class);
        PowerMockito.mockStatic(ColumnDataType.class);
        MockitoAnnotations.initMocks(this);

        joins.add(join);
        columns.add(column1);
        constraints.add(constraint);
        selectColumns.add(selectColumn1);

        Table hiveTable = HiveTable.builder().name("test_table").db("test_db").columns(columns).build();
        tables.add(hiveTable);

        when(join.getLeftTable()).thenReturn(hiveTable);
        when(join.getRightTable()).thenReturn(hiveTable);
        when(join.getJoinColumns()).thenReturn(columnMap);
        when(join.getJoinClause()).thenReturn(constraints);

        when(constraint.getTableName()).thenReturn(tableName);
        when(constraint.getConstraintColumn()).thenReturn(column1);
        when(constraint.getInValues()).thenReturn(new HashSet<>());
        when(constraint.getConstraintType()).thenReturn(ConstraintType.IN);

        when(column1.getDefaultValue()).thenReturn(1L);
        when(column1.getColumnName()).thenReturn(columnName1);
        when(column1.getColumnDataType()).thenReturn(ColumnDataType.INT);
        when(column2.getColumnDataType()).thenReturn(ColumnDataType.INT);

        when(selectColumn1.getName()).thenReturn(columnName1);
        when(selectColumn1.getIsGranularity()).thenReturn(true);
        when(selectColumn1.getAlias()).thenReturn("column1Alias");
        when(selectColumn1.getHiveAggregationType()).thenReturn(AggregationType.NA.NA);

        when(selectColumn2.getName()).thenReturn(columnName2);
        when(selectColumn2.getIsGranularity()).thenReturn(false);
        when(selectColumn2.getAlias()).thenReturn("column2Alias");
        when(selectColumn2.getHiveAggregationType()).thenReturn(AggregationType.COUNT);

        when(subQuery.constructQuery()).thenReturn("selectQuery");
        PowerMockito.when(ColumnDataType.defaultValue(ColumnDataType.INT)).thenReturn("0");
        PowerMockito.when(ConstraintType.getConstraints(any(), any())).thenReturn("refresh_id = 1");
    }

    @Test
    public void testGetQueryType() {
        this.selectQuery = spy(new SelectQuery(joins, constraints, groupByColumns, tables, selectColumns, QueryBehaviorType.JOIN));
        assertEquals(QueryType.SELECT_QUERY, selectQuery.getQueryType());
    }

    @Test
    public void testConstructQueryFailure() throws Exception {
        Table hiveTable1 = HiveTable.builder().name("test_table").db("test_db").columns(columns).build();
        tables.add(hiveTable1);
        selectColumns.add(selectColumn2);

        this.selectQuery = spy(new SelectQuery(joins, constraints, groupByColumns, tables, selectColumns, QueryBehaviorType.NO_JOIN));
        boolean isException = false;
        try {
            selectQuery.constructQuery();
        } catch (IllegalArgumentException e) {
            isException = true;
            assertEquals(e.getMessage(), "NO_JOIN Can't have more than one tables");
        }

        assertTrue(isException);
        verify(selectColumn1, times(2)).getName();
        verify(selectColumn1, times(2)).getIsGranularity();
        verify(selectColumn1).getAlias();
        verify(selectColumn2, times(3)).getName();
        verify(selectColumn2, times(2)).getIsGranularity();
        verify(column1, times(5)).getColumnName();
    }

    @Test
    public void testConstructQuerySuccessCase1() throws Exception {
        selectColumns.add(selectColumn2);
        this.selectQuery = spy(new SelectQuery(joins, constraints, groupByColumns, tables, selectColumns, QueryBehaviorType.NO_JOIN));

        String expected = selectQuery.constructQuery();
        assertNotNull(expected);
        assertEquals(expected, "SELECT DISTINCT \ntest_table.`column1` AS `column1Alias`,COUNT(*) FROM test_db.test_table WHERE refresh_id = 1  ");
        verify(selectColumn1, times(2)).getName();
        verify(selectColumn1, times(2)).getIsGranularity();
        verify(selectColumn1).getAlias();
        verify(selectColumn2, times(3)).getName();
        verify(selectColumn2, times(2)).getIsGranularity();
        verify(column1, times(4)).getColumnName();
        verify(constraint).getTableName();
    }

    @Test
    public void testConstructQuerySuccessCase2() throws Exception {
        columns.add(column2);
        selectColumns.add(selectColumn2);

        when(column2.getColumnName()).thenReturn(columnName1);
        when(selectColumn2.getName()).thenReturn(columnName1);
        when(selectColumn2.getHiveAggregationType()).thenReturn(AggregationType.NA.MAX);

        this.selectQuery = spy(new SelectQuery(joins, constraints, groupByColumns, tables, selectColumns, QueryBehaviorType.CARTESIAN_MULTIPLICATION));
        String expected = selectQuery.constructQuery();
        assertNotNull(expected);
        verify(selectColumn1, times(2)).getName();
        verify(selectColumn1, times(2)).getIsGranularity();
        verify(selectColumn1).getAlias();
        verify(selectColumn2, times(4)).getName();
        verify(selectColumn2, times(2)).getIsGranularity();
        verify(selectColumn2, times(3)).getHiveAggregationType();
        verify(column1, times(5)).getColumnName();
        verify(constraint).getTableName();
    }

    @Test
    public void testConstructQuerySuccessCase3() throws Exception {
        Table hiveTable1 = HiveTable.builder().name("test_table").db("test_db").columns(columns).build();
        tables.add(hiveTable1);

        this.selectQuery = spy(new SelectQuery(joins, constraints, groupByColumns, tables, selectColumns, QueryBehaviorType.CROSS_JOIN));
        String expected = selectQuery.constructQuery();
        assertNotNull(expected);
        assertEquals(expected, "SELECT  \n" + "test_table.`column1` AS `column1Alias` FROM test_db.test_table JOIN test_db.test_table ON refresh_id = 1  AND refresh_id = 1 ");
        verify(selectColumn1, times(2)).getName();
        verify(selectColumn1, times(2)).getIsGranularity();
        verify(selectColumn1).getAlias();
        verify(column1, times(5)).getColumnName();
        verify(constraint, times(6)).getTableName();
        verify(constraint, times(2)).getConstraintColumn();
    }

    @Test
    public void testConstructQuerySuccessCase4() throws Exception {
        columns.add(column2);
        selectColumns.add(selectColumn2);

        when(column2.getColumnName()).thenReturn(columnName3);
        when(column2.getDefaultValue()).thenReturn("");
        when(selectColumn2.getName()).thenReturn(columnName3);
        when(selectColumn2.getHiveAggregationType()).thenReturn(AggregationType.MAX);

        columnMap.put(column1, column1);
        groupByColumns.add(columnName1);
        Table hiveTable = HiveTable.builder().name("test_table").db("test_db").columns(columns).subQuery(subQuery).build();
        when(join.getLeftTable()).thenReturn(hiveTable);
        when(join.getRightTable()).thenReturn(hiveTable);


        this.selectQuery = spy(new SelectQuery(joins, constraints, groupByColumns, tables, selectColumns, QueryBehaviorType.JOIN));
        String expected = selectQuery.constructQuery();
        assertNotNull(expected);

        verify(selectColumn1, times(2)).getName();
        verify(selectColumn1, times(2)).getIsGranularity();
        verify(selectColumn2, times(4)).getName();
        verify(selectColumn2, times(2)).getIsGranularity();
        verify(selectColumn2, times(3)).getHiveAggregationType();

        verify(selectColumn1).getAlias();
        verify(column1, times(9)).getColumnName();
        verify(constraint, times(2)).getConstraintColumn();
        verify(join, times(6)).getLeftTable();
        verify(join, times(3)).getRightTable();
        verify(join).getJoinColumns();
        verify(join, times(2)).getJoinClause();
        verify(constraint).getConstraintType();
        verify(constraint, times(2)).getConstraintColumn();
    }

    @Test
    public void testConstructQuerySuccessCase6() throws Exception {
        when(constraint.getConstraintColumn()).thenReturn(column2);
        columnMap.put(column1, column1);
        groupByColumns.add(columnName1);

        this.selectQuery = spy(new SelectQuery(joins, constraints, groupByColumns, tables, selectColumns, QueryBehaviorType.JOIN));
        selectQuery.constructQuery();
        verify(selectColumn1, times(2)).getName();
        verify(selectColumn1, times(2)).getIsGranularity();
        verify(selectColumn1).getAlias();
        verify(column1, times(6)).getColumnName();
        verify(constraint, times(2)).getConstraintColumn();
        verify(join, times(5)).getLeftTable();
        verify(join, times(4)).getRightTable();
        verify(join).getJoinColumns();
        verify(join, times(2)).getJoinClause();
        verify(constraint).getConstraintType();
        verify(constraint, times(2)).getConstraintColumn();
    }

}
