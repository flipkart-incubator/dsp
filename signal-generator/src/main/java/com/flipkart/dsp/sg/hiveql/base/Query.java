package com.flipkart.dsp.sg.hiveql.base;

import com.flipkart.dsp.sg.hiveql.query.QueryType;
import com.flipkart.dsp.sg.exceptions.InvalidQueryException;

/**
 */
public interface Query {
    public QueryType getQueryType();

    public String constructQuery() throws InvalidQueryException;
}
