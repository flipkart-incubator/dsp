package com.flipkart.dsp.sg.hiveql.core;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * +
 */
public class HiveColumnTest {

    private HiveColumn hiveColumn = HiveColumn.builder().name("columnName").isPartition(true)
            .columnDataType(ColumnDataType.INT).defaultValue(1L).build();

    @Test
    public void test() {
        assertTrue(hiveColumn.isPartitionColumn());
        assertEquals(hiveColumn.getDefaultValue(), 1L);
        assertEquals(hiveColumn.getColumnName(), "columnName");
        assertEquals(hiveColumn.getColumnDataType(), ColumnDataType.INT);
    }
}
