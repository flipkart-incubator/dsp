package com.flipkart.dsp.sg.hiveql.query;

import com.flipkart.dsp.sg.hiveql.base.Table;
import com.flipkart.dsp.sg.hiveql.core.HiveTable;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;

/**
 * +
 */
public class DropQueryTest {
    private DropQuery dropQuery;

    @Before
    public void setUp() {
        Table hiveTable = HiveTable.builder().name("test_table").db("test_db").build();
        this.dropQuery = spy(new DropQuery(hiveTable));
    }

    @Test
    public void testGetQueryType() {
        assertEquals(QueryType.DROP_QUERY, dropQuery.getQueryType());
    }

    @Test
    public void testConstructQuery() {
        String expected = dropQuery.constructQuery();
        assertEquals(expected, "DROP TABLE test_db.test_table");
    }
}
