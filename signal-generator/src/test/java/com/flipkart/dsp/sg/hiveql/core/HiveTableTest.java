package com.flipkart.dsp.sg.hiveql.core;

import com.flipkart.dsp.sg.hiveql.base.Column;
import com.flipkart.dsp.sg.hiveql.query.SelectQuery;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.LinkedList;

import static com.flipkart.dsp.utils.Constants.comma;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

/**
 * +
 */
public class HiveTableTest {
    @Mock private Column column;
    @Mock private SelectQuery selectQuery;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void test() {
        LinkedList<Column> columns = new LinkedList<>();
        columns.add(column);
        HiveTable hiveTable = HiveTable.builder().db("test_db").name("test_table").columns(columns).delimiter(comma).build();
        hiveTable.setRefreshId(1L);
        hiveTable.setSubQuery(selectQuery);

        when(column.getColumnName()).thenReturn("column1");

        assertEquals(hiveTable.getRefreshId().longValue(), 1L);
        assertEquals(hiveTable.getDbName(), "test_db");
        assertEquals(hiveTable.getTableName(), "test_table");
        assertEquals(hiveTable.getColumns(), columns);
        assertTrue(hiveTable.containsColumn(column));
        assertTrue(hiveTable.containsColumn("column1"));
        assertEquals(hiveTable.getColumn("column1"), column);
        assertNull(hiveTable.getColumn("column2"));
        assertEquals(hiveTable.getSubQuery(), selectQuery);
        assertTrue(hiveTable.hasSubQuery());
        assertEquals(hiveTable.getDelimiter(), comma);
    }
}
