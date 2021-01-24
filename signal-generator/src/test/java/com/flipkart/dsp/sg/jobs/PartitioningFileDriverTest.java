package com.flipkart.dsp.sg.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({Path.class, PartitioningFileDriver.class, Job.class, FileInputFormat.class, FileOutputFormat.class, MultipleOutputs.class})
public class PartitioningFileDriverTest {

    @Mock private Job job;
    @Mock private Configuration configuration;

    private PartitioningFileDriver partitioningFileDriver;

    @Before
    public void setUp() {
        PowerMockito.mockStatic(Path.class);
        PowerMockito.mockStatic(Job.class);
        PowerMockito.mockStatic(FileInputFormat.class);
        PowerMockito.mockStatic(MultipleOutputs.class);
        PowerMockito.mockStatic(FileOutputFormat.class);
        MockitoAnnotations.initMocks(this);
        this.partitioningFileDriver = spy(new PartitioningFileDriver("dataFrameId", ",", new ArrayList<>(), new ArrayList<>()));
    }

    @Test
    public void testRunSuccess() throws Exception {
        String[] args = new String[2];
        args[0] = "sourcePath"; args[1] = "destinationPath";

        when(partitioningFileDriver.getConf()).thenReturn(configuration);
        PowerMockito.when(Job.getInstance(any(Configuration.class), any())).thenReturn(job);
        PowerMockito.doNothing().when(FileInputFormat.class, "addInputPath", any(), any());
        PowerMockito.doNothing().when(FileOutputFormat.class, "setOutputPath", any(), any());
        PowerMockito.doNothing().when(MultipleOutputs.class, "addNamedOutput", any(), any(), any(), any(), any());
        when(job.waitForCompletion(true)).thenReturn(true);

        int actual = partitioningFileDriver.run(args);
        assertEquals(actual, 0);
        verify(partitioningFileDriver, times(1)).getConf();
        verifyStatic(Job.class, times(1));
        Job.getInstance(any(Configuration.class), any());
        verifyStatic(FileInputFormat.class, times(1));
        FileInputFormat.addInputPath(any(), any());
        verifyStatic(FileOutputFormat.class, times(1));
        FileOutputFormat.setOutputPath(any(), any());
        verifyStatic(MultipleOutputs.class, times(1));
        MultipleOutputs.addNamedOutput(any(), any(), any(), any(), any());
        verify(job, times(1)).waitForCompletion(true);
    }
}
