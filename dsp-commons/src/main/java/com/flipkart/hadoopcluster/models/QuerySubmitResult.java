package com.flipkart.hadoopcluster2.models;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@AllArgsConstructor
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
@ToString
public abstract class QuerySubmitResult implements Serializable {
    protected static final long serialVersionUID = -5235485242067954079L;

    /**
     * Associated query handle
     */
    private final QueryHandle queryHandle;
}
