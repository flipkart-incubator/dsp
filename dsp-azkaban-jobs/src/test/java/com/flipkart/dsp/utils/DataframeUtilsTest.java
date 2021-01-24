package com.flipkart.dsp.utils;

import com.flipkart.dsp.entities.sg.dto.DataFrameColumnType;
import com.flipkart.dsp.entities.sg.dto.SGUseCasePayload;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * +
 */
public class DataframeUtilsTest {

    @Mock private SGUseCasePayload sgUseCasePayload;

    private String partition = "partition";
    private String workflowName = "workflowName";
    private List<String> partitions = new ArrayList<>();
    private List<SGUseCasePayload> sgUseCasePayloads = new ArrayList<>();
    private LinkedHashMap<String, DataFrameColumnType> columnMetaData = new LinkedHashMap<>();

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        sgUseCasePayloads.add(sgUseCasePayload);
        columnMetaData.put(partition, DataFrameColumnType.RANGE);
    }

    @Test
    public void testGetReferenceDFForParallelismSuccess() {
        partitions.add(partition);
        when(sgUseCasePayload.getColumnMetaData()).thenReturn(columnMetaData);
        SGUseCasePayload expected = DataframeUtils.getReferenceDFForParallelism(workflowName, partitions, sgUseCasePayloads);
        assertNotNull(expected);
        assertEquals(sgUseCasePayload, expected);
        verify(sgUseCasePayload, times(2)).getColumnMetaData();
    }

    @Test
    public void testGetReferenceDFForParallelismFailure() {
        boolean isException = false;
        when(sgUseCasePayload.getColumnMetaData()).thenReturn(columnMetaData);
        try {
            DataframeUtils.getReferenceDFForParallelism(workflowName, partitions, sgUseCasePayloads);
        } catch (RuntimeException e) {
            isException = true;
            assertEquals(e.getMessage(), "None of the data frames partitions matching with use case partition!!");
        }

        assertTrue(isException);
        verify(sgUseCasePayload, times(1)).getColumnMetaData();
    }
}
