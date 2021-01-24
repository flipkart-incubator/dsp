package com.flipkart.hadoopcluster2.models;

import lombok.Getter;

import java.util.Iterator;

public class QueryResultIterator implements Iterator<ResultRow> {
    /**
     * Meta data of the result set
     */
    @Getter
    private final QueryResultMeta metaData;
    /**
     * Iterator for the result set
     */
    private final Iterator<ResultRow> resultRowIterator;

    public QueryResultIterator(final QueryResultMeta metaData, final Iterator<ResultRow> resultRowIterator) {
        this.metaData = metaData;
        /**
         * TODO Iterator having active connections will have to be cleaned up above this level. (LEAK)
         */
        this.resultRowIterator = resultRowIterator;
    }

    @Override
    public boolean hasNext() {
        return resultRowIterator.hasNext();
    }

    @Override
    public ResultRow next() {
        return resultRowIterator.next();
    }
}
