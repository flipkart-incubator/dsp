package com.flipkart.dsp.sg.jobs;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.flipkart.dsp.utils.Constants.*;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * +
 */
public class PartitioningFileMapperTest {


    @Mock private LongWritable key;
    @Mock private Mapper.Context context;
    @Mock private Configuration configuration;

    private List<String> headers = new ArrayList<>();
    private Text value = new Text("Key#value");
    private List<String> partitions = new ArrayList<>();
    private PartitioningFileMapper partitioningFileMapper;
    private ObjectMapper objectMapper = new ObjectMapper();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        this.partitioningFileMapper = spy(new PartitioningFileMapper());

        headers.add("column1");
        partitions.add("column1");

        when(context.getConfiguration()).thenReturn(configuration);
        when(configuration.get(FIELD_DELIMITER)).thenReturn("#");
        when(configuration.get(HEADERS_KEY)).thenReturn(objectMapper.writeValueAsString(headers));
        when(configuration.get(PARTITION_COLUMNS)).thenReturn(objectMapper.writeValueAsString(partitions));
    }

    @Test
    public void testMapSuccess() throws Exception {
        doNothing().when(context).write(any(), any());
        partitioningFileMapper.setup(context);
        partitioningFileMapper.map(key, value, context);

        verify(context, times(3)).getConfiguration();
        verify(configuration).get(FIELD_DELIMITER);
        verify(configuration).get(HEADERS_KEY);
        verify(configuration).get(PARTITION_COLUMNS);
        verify(context).write(any(), any());
    }

    @Test
    public void testMapFailure() throws Exception {
        boolean isException = false;
        headers.add("column2");
        when(configuration.get(HEADERS_KEY)).thenReturn(objectMapper.writeValueAsString(headers));
        doThrow(new IOException()).when(context).write(any(), any());
        doThrow(new InterruptedException()).when(context).write(any(), any());
        partitioningFileMapper.setup(context);
        try {
            partitioningFileMapper.map(key, value, context);
        } catch (Exception e) {
            isException = true;
        }

        assertTrue(isException);
        verify(context, times(3)).getConfiguration();
        verify(configuration).get(FIELD_DELIMITER);
        verify(configuration).get(HEADERS_KEY);
        verify(configuration).get(PARTITION_COLUMNS);
        verify(context).write(any(), any());
    }
}
