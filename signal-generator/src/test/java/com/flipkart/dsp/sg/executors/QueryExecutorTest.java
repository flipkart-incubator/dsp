package com.flipkart.dsp.sg.executors;

import com.flipkart.dsp.qe.clients.HiveClient;
import com.flipkart.dsp.qe.exceptions.HiveClientException;
import com.flipkart.dsp.sg.exceptions.QueryExecutionException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static com.flipkart.dsp.utils.Constants.PRODUCTION_HIVE_QUEUE;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * +
 */
public class QueryExecutorTest {
    @Mock private HiveClient hiveClient;
    private HiveQueryExecutor hiveQueryExecutor;

    private String query = "testQuery";
    private List<String> queryList = new ArrayList<>();

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        this.hiveQueryExecutor = spy(new HiveQueryExecutor(hiveClient));
        queryList.add(query);
    }

    @Test
    public void testExecuteListSuccess() throws Exception {
        doNothing().when(hiveClient).executeQuery(query);
        hiveQueryExecutor.executeList(queryList, PRODUCTION_HIVE_QUEUE);
        verify(hiveClient, times(1)).executeQuery(query);
    }

    @Test
    public void testExecuteListFailure() throws Exception {
        doThrow(new HiveClientException("Error")).when(hiveClient).executeQuery(query);

        boolean isException = false;
        try {
            hiveQueryExecutor.executeList(queryList, PRODUCTION_HIVE_QUEUE);
        } catch (QueryExecutionException e) {
            isException = true;
            assertTrue(e.getMessage().contains("Failed execute query " + query + " With following error"));
        }
        assertTrue(isException);
        verify(hiveClient, times(1)).executeQuery(query);
    }

}
