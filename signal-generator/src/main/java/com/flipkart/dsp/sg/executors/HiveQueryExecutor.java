package com.flipkart.dsp.sg.executors;

import com.flipkart.dsp.qe.clients.HiveClient;
import com.flipkart.dsp.qe.exceptions.HiveClientException;
import com.flipkart.dsp.sg.exceptions.QueryExecutionException;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor(onConstructor = @__(@Inject))
@Slf4j
public class HiveQueryExecutor extends QueryExecutor {

    private final HiveClient hiveClient;

    @Override
    void execute(String query, String hiveQueue) throws QueryExecutionException {
        try {
            hiveClient.setQueue(hiveQueue);
            hiveClient.executeQuery(query);
        } catch (HiveClientException e) {
            throw new QueryExecutionException("Failed execute query " + query + " With following error", e);
        }
    }
}
