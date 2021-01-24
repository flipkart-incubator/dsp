package com.flipkart.dsp.sg.hiveql.query;

import com.flipkart.dsp.sg.hiveql.base.Column;
import com.flipkart.dsp.sg.hiveql.core.ColumnDataType;
import com.flipkart.dsp.sg.hiveql.core.HiveTable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.LinkedList;

import static com.flipkart.dsp.utils.Constants.comma;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * +
 */
public class CreateQueryTest {

    @Mock private Column column;
    @Mock private Column partitionColumn;

    private CreateQuery createQuery;
    private LinkedList<Column> columns = new LinkedList<>();

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        columns.add(column);
        columns.add(partitionColumn);
        HiveTable hiveTable = HiveTable.builder().db("db_name").delimiter(comma).name("table_name").columns(columns).build();
        this.createQuery = spy(new CreateQuery(hiveTable));
    }

    @Test
    public void testGetQueryType() {
        assertEquals(QueryType.CREATE_QUERY, createQuery.getQueryType());
    }

    @Test
    public void testConstructQuery() {
        when(column.isPartitionColumn()).thenReturn(false);
        when(column.getColumnName()).thenReturn("column1");
        when(column.getColumnDataType()).thenReturn(ColumnDataType.INT);
        when(partitionColumn.isPartitionColumn()).thenReturn(true);
        when(partitionColumn.getColumnName()).thenReturn("column2");
        when(partitionColumn.getColumnDataType()).thenReturn(ColumnDataType.STRING);

        createQuery.constructQuery();
        verify(column, times(3)).isPartitionColumn();
        verify(column).getColumnName();
        verify(column).getColumnDataType();
        verify(partitionColumn, times(3)).isPartitionColumn();
        verify(partitionColumn).getColumnName();
        verify(partitionColumn).getColumnDataType();

    }
}

