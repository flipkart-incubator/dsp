package com.flipkart.dsp.sg.hiveql.query;

import com.flipkart.dsp.sg.hiveql.base.Column;
import com.flipkart.dsp.sg.hiveql.base.Table;
import com.flipkart.dsp.sg.hiveql.core.ColumnDataType;
import com.flipkart.dsp.sg.hiveql.core.HiveTable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.LinkedHashSet;
import java.util.LinkedList;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * +
 */
public class InsertQueryTest {
    @Mock private Column column;
    @Mock private SelectQuery selectQuery;
    @Mock private SelectColumn selectColumn;

    private InsertQuery insertQuery;
    private String columnName = "column1";
    private LinkedList<Column> columns = new LinkedList<>();
    private LinkedHashSet<String> partitions = new LinkedHashSet<>();;
    private LinkedHashSet<SelectColumn> selectColumns = new LinkedHashSet<>();;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        Table hiveTable =  HiveTable.builder().name("test_table").db("test_db").columns(columns).build();
        this.insertQuery = spy(new InsertQuery(hiveTable, partitions, selectQuery));

        columns.add(column);
        partitions.add(columnName);
        selectColumns.add(selectColumn);
    }

    @Test
    public void testGetQueryType() {
        assertEquals(QueryType.INSERT_QUERY, insertQuery.getQueryType());
    }

    @Test
    public void testConstructQuerySuccess() throws Exception {
        when(selectQuery.constructQuery()).thenReturn("select column1 from test_db.test_table where refresh_id=1");
        when(column.isPartitionColumn()).thenReturn(true);
        when(column.getColumnName()).thenReturn(columnName);
        when(column.getColumnDataType()).thenReturn(ColumnDataType.INT);
        when(selectQuery.getSelectColumns()).thenReturn(selectColumns);
        when(selectColumn.getName()).thenReturn(columnName);

        String expected = insertQuery.constructQuery();
        assertEquals(expected, "INSERT OVERWRITE TABLE test_db.test_table PARTITION(column1)\n" +
                "select column1 from test_db.test_table where refresh_id=1");
        verify(column).isPartitionColumn();
        verify(column, times(2)).getColumnName();
        verify(selectQuery, times(2)).getSelectColumns();
        verify(selectColumn).getName();
        verify(selectQuery).constructQuery();
    }
}
