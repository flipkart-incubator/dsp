package com.flipkart.dsp.sg.hiveql.core;

import com.flipkart.dsp.models.sg.SignalDataType;

/**
 */

public enum ColumnDataType {
    TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, CHAR,
    STRING, VARCHAR, BINARY, BOOLEAN, TIMESTAMP, DATE;

    public static ColumnDataType from(SignalDataType signalDataType) {
        switch (signalDataType) {
            case TEXT:
            case TIME_DAY:
            case TIME_WEEK:
            case TIME_MONTH:
            case TIME_YEAR:
                return ColumnDataType.STRING;
            case DOUBLE:
            case FLOAT:
                return ColumnDataType.DOUBLE;
            case INTEGER:
                return ColumnDataType.INT;
            case BIG_INTEGER:
                return ColumnDataType.BIGINT;
            case BOOLEAN:
                return ColumnDataType.BOOLEAN;
            case DATETIME:
                return ColumnDataType.TIMESTAMP;
            case DATE:
                return ColumnDataType.DATE;
            default:
                throw new IllegalArgumentException("Invalid signal data type to column data type mapping");
        }
    }

    public static boolean isQuoteRequired(ColumnDataType columnDataType) {
        switch (columnDataType) {
            case CHAR:
            case STRING:
            case VARCHAR:
            case DATE:
            case TIMESTAMP:
                return true;
            default:
                return false;
        }
    }

    public static String defaultValue(ColumnDataType columnDataType) {
        switch (columnDataType) {
            case INT:
            case BIGINT:
                return "0";
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return "0.0";
            case STRING:
            case VARCHAR:
                return "'NA'";
            case BINARY:
            case BOOLEAN:
            default:
                return "";
        }
    }
}
