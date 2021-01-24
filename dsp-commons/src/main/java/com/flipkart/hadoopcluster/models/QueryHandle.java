package com.flipkart.hadoopcluster2.models;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;

@JsonDeserialize(builder = QueryHandle.QueryHandleBuilder.class)
@Builder
@Getter
@Slf4j
@ToString
public class QueryHandle implements Serializable {
    /**
     * String representation of the handle
     */
    private final String handle;

    @Override
    public String toString() {
        return handle;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryHandle that = (QueryHandle) o;

        return !(handle != null ? !handle.equals(that.handle) : that.handle != null);

    }

    @Override
    public int hashCode() {
        return handle != null ? handle.hashCode() : 0;
    }

    @JsonPOJOBuilder(withPrefix = "")
    public static final class QueryHandleBuilder {
    }
}
