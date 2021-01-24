package com.flipkart.hadoopcluster2.models;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
@JsonDeserialize(builder = Column.ColumnBuilder.class)
public class Column {
    /**
     * Name of the column
     */
    private final String name;
    /**
     * Data type of the column
     */
    private final ClientTypeSignature dataType;

    @JsonPOJOBuilder(withPrefix = "")
    public static final class ColumnBuilder {
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Column column = (Column) o;

        if (name != null ? !name.equalsIgnoreCase(column.name) : column.name != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (dataType != null ? dataType.hashCode() : 0);
        return result;
    }
}
