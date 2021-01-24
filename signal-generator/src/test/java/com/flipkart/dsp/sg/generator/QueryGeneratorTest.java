package com.flipkart.dsp.sg.generator;

import com.flipkart.dsp.dto.DataFrameGenerateRequest;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.models.sg.DataFrameScope;
import com.flipkart.dsp.models.sg.SGType;
import com.flipkart.dsp.sg.hiveql.base.Table;
import org.javatuples.Pair;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

/**
 * +
 */
public class QueryGeneratorTest {

    @Mock private Table table;
    @Mock private DataFrame dataFrame;
    @Mock private DataFrameScope dataFrameScope;
    @Mock private FullQueryBuilder fullQueryBuilder;
    @Mock private SingleTableQueryBuilder singleTableQueryBuilder;
    @Mock private DataFrameGenerateRequest dataFrameGenerateRequest;

    private Long runId = 1L;
    private QueryGenerator queryGenerator;
    private Set<DataFrameScope> dataFrameScopes = new HashSet<>();
    Pair<Table, List<String>> query;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        this.queryGenerator = spy(new QueryGenerator(fullQueryBuilder, singleTableQueryBuilder));

        query = Pair.with(table, new ArrayList<>());
        dataFrameScopes.add(dataFrameScope);
        when(table.getDbName()).thenReturn("test_db");
        when(table.getTableName()).thenReturn("test_table");
    }

    @Test
    public void testGenerateQueryCase1() throws Exception {
        when(dataFrame.getSgType()).thenReturn(SGType.NO_QUERY);
        when(singleTableQueryBuilder.buildQuery(runId, dataFrame, dataFrameGenerateRequest, dataFrameScopes)).thenReturn(query);

        Pair<Table, List<String>> expected =  queryGenerator.generateQuery(runId, dataFrame, dataFrameGenerateRequest, dataFrameScopes);
        assertNotNull(expected);
        assertEquals(expected.getValue0(), table);
        verify(dataFrame, times(2)).getSgType();
        verify(singleTableQueryBuilder).buildQuery(runId, dataFrame, dataFrameGenerateRequest, dataFrameScopes);
        verify(table).getDbName();
        verify(table).getTableName();
    }

    @Test
    public void testGenerateQueryCase2() throws Exception {
        when(dataFrame.getSgType()).thenReturn(SGType.SINGLE_TABLE_QUERY);
        when(singleTableQueryBuilder.buildQuery(runId, dataFrame, dataFrameGenerateRequest, dataFrameScopes)).thenReturn(query);

        Pair<Table, List<String>> expected =  queryGenerator.generateQuery(runId, dataFrame, dataFrameGenerateRequest, dataFrameScopes);
        assertNotNull(expected);
        assertEquals(expected.getValue0(), table);
        verify(dataFrame, times(2)).getSgType();
        verify(singleTableQueryBuilder).buildQuery(runId, dataFrame, dataFrameGenerateRequest, dataFrameScopes);
        verify(table).getDbName();
        verify(table).getTableName();
    }

    @Test
    public void testGenerateQueryCase3() throws Exception {
        when(dataFrame.getSgType()).thenReturn(SGType.FULL_QUERY);
        when(fullQueryBuilder.buildQuery(runId, dataFrame, dataFrameGenerateRequest, dataFrameScopes)).thenReturn(query);

        Pair<Table, List<String>> expected =  queryGenerator.generateQuery(runId, dataFrame, dataFrameGenerateRequest, dataFrameScopes);
        assertNotNull(expected);
        assertEquals(expected.getValue0(), table);
        verify(dataFrame, times(2)).getSgType();
        verify(fullQueryBuilder).buildQuery(runId, dataFrame, dataFrameGenerateRequest, dataFrameScopes);
        verify(table).getDbName();
        verify(table).getTableName();
    }

}
