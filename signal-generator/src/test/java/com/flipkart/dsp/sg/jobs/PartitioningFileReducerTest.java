package com.flipkart.dsp.sg.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Iterator;

import static com.flipkart.dsp.utils.Constants.DATAFRAME_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.verifyNew;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({MultipleOutputs.class, PartitioningFileReducer.class})
public class PartitioningFileReducerTest {
    @Mock private Iterator iterator;
    @Mock private Iterable<Text> values;
    @Mock private Reducer.Context context;
    @Mock private Configuration configuration;
    @Mock private MultipleOutputs multipleOutputs;

    private Text key = new Text("key");
    private PartitioningFileReducer partitioningFileReducer;

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(MultipleOutputs.class);
        MockitoAnnotations.initMocks(this);
        this.partitioningFileReducer = Mockito.spy(new PartitioningFileReducer());

        PowerMockito.whenNew(MultipleOutputs.class).withArguments(context).thenReturn(multipleOutputs);
        when(context.getConfiguration()).thenReturn(configuration);
        when(configuration.get(DATAFRAME_KEY)).thenReturn("dataFrameId");
    }

    @Test
    public void testReduce() throws Exception {
        when(values.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true,false);
        when(iterator.next()).thenReturn(key);
        doNothing().when(multipleOutputs).write(any(), any(), any(), any());

        partitioningFileReducer.setup(context);
        partitioningFileReducer.reduce(key, values, context);
        verifyNew(MultipleOutputs.class).withArguments(context);
        verify(context).getConfiguration();
        verify(configuration).get(DATAFRAME_KEY);
        verify(values).iterator();
        verify(iterator, times(2)).hasNext();
        verify(iterator).next();
        verify(multipleOutputs).write(any(), any(), any(), any());
    }

    @Test
    public void testCleanUp() throws Exception {
        doNothing().when(multipleOutputs).close();
        partitioningFileReducer.setup(context);
        partitioningFileReducer.cleanup(context);
        verifyNew(MultipleOutputs.class).withArguments(context);
        verify(context).getConfiguration();
        verify(configuration).get(DATAFRAME_KEY);
        verify(multipleOutputs).close();
    }
}
