package com.flipkart.dsp.sg.hiveql.core;

import com.flipkart.dsp.sg.hiveql.base.Column;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

/**
 */

@Builder
@EqualsAndHashCode
@RequiredArgsConstructor
public class HiveColumn implements Column {
    private final String name;
    private final ColumnDataType columnDataType;
    private final Object defaultValue;
    private final boolean isPartition;

    @Override
    public String getColumnName() {
        return name;
    }

    @Override
    public ColumnDataType getColumnDataType() {
        return columnDataType;
    }

    @Override
    public Object getDefaultValue() {
        return defaultValue;
    }

    @Override
    public boolean isPartitionColumn() {
        return isPartition;
    }

}
