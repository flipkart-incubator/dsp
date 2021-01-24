package com.flipkart.dsp.sg.executors;

import com.flipkart.dsp.sg.exceptions.QueryExecutionException;

import java.util.List;

abstract class QueryExecutor {

    abstract void execute(String query, String hiveQueue) throws QueryExecutionException;

    public void executeList(List<String> queryList, String hiveQueue) throws QueryExecutionException {
        for (String query: queryList) {
            execute(query, hiveQueue);
        }
    }
}
