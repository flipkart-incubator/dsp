package com.flipkart.dsp.sg.hiveql.base;

import com.flipkart.dsp.sg.hiveql.core.ColumnDataType;

/**
 */
public interface Column {

    public String getColumnName();

    public ColumnDataType getColumnDataType();

    public Object getDefaultValue();

    public boolean isPartitionColumn();

}
