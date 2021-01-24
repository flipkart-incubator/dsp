package com.flipkart.dsp.sg.hiveql.core;

import com.flipkart.dsp.models.sg.SignalDataType;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 */
public class ColumnDataTypeTest {

    @Test
    public void testSignalToHiveColumnMapping() {
        assertEquals(ColumnDataType.from(SignalDataType.TEXT), ColumnDataType.STRING);
        assertEquals(ColumnDataType.from(SignalDataType.DOUBLE), ColumnDataType.DOUBLE);
        assertEquals(ColumnDataType.from(SignalDataType.INTEGER), ColumnDataType.INT);
        assertEquals(ColumnDataType.from(SignalDataType.FLOAT), ColumnDataType.DOUBLE);
        assertEquals(ColumnDataType.from(SignalDataType.BIG_INTEGER), ColumnDataType.BIGINT);
        assertEquals(ColumnDataType.from(SignalDataType.TIME_DAY), ColumnDataType.STRING);
        assertEquals(ColumnDataType.from(SignalDataType.TIME_WEEK), ColumnDataType.STRING);
        assertEquals(ColumnDataType.from(SignalDataType.TIME_MONTH), ColumnDataType.STRING);
        assertEquals(ColumnDataType.from(SignalDataType.BOOLEAN), ColumnDataType.BOOLEAN);
        assertEquals(ColumnDataType.from(SignalDataType.DATETIME), ColumnDataType.TIMESTAMP);
        assertEquals(ColumnDataType.from(SignalDataType.DATE), ColumnDataType.DATE);
    }

    @Test
    public void testIsQuoteRequired() {
        assertFalse(ColumnDataType.isQuoteRequired(ColumnDataType.TINYINT));
        assertTrue(ColumnDataType.isQuoteRequired(ColumnDataType.CHAR));
        assertTrue(ColumnDataType.isQuoteRequired(ColumnDataType.STRING));
        assertTrue(ColumnDataType.isQuoteRequired(ColumnDataType.VARCHAR));
        assertTrue(ColumnDataType.isQuoteRequired(ColumnDataType.DATE));
        assertTrue(ColumnDataType.isQuoteRequired(ColumnDataType.TIMESTAMP));
    }

    @Test
    public void testDefaultValue() {
        assertEquals(ColumnDataType.defaultValue(ColumnDataType.INT), "0");
        assertEquals(ColumnDataType.defaultValue(ColumnDataType.BIGINT), "0");
        assertEquals(ColumnDataType.defaultValue(ColumnDataType.FLOAT), "0.0");
        assertEquals(ColumnDataType.defaultValue(ColumnDataType.DOUBLE), "0.0");
        assertEquals(ColumnDataType.defaultValue(ColumnDataType.DECIMAL), "0.0");
        assertEquals(ColumnDataType.defaultValue(ColumnDataType.STRING), "'NA'");
        assertEquals(ColumnDataType.defaultValue(ColumnDataType.VARCHAR), "'NA'");
        assertEquals(ColumnDataType.defaultValue(ColumnDataType.BINARY), "");
        assertEquals(ColumnDataType.defaultValue(ColumnDataType.BOOLEAN), "");
        assertEquals(ColumnDataType.defaultValue(ColumnDataType.CHAR), "");
    }
}
