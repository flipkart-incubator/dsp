package com.flipkart.dsp.cephingestion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

import static com.flipkart.dsp.utils.Constants.comma;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({PartitionFileMergingDriver.class, SequenceFile.class, Job.class, Path.class, FileInputFormat.class, FileOutputFormat.class})
public class PartitionFileMergingDriverTest {

    @Mock private Job job;
    @Mock private Path path;
    @Mock private FileSystem fileSystem;
    @Mock private SequenceFile.Writer writer;
    @Mock private Configuration configuration;
    private List<String> files = new ArrayList<>();
    private PartitionFileMergingDriver partitionFileMergingDriver;

    @Before
    public void setUp() {
        files.add("file");
        PowerMockito.mockStatic(Path.class);
        PowerMockito.mockStatic(Job.class);
        PowerMockito.mockStatic(SequenceFile.class);
        PowerMockito.mockStatic(FileInputFormat.class);
        PowerMockito.mockStatic(FileOutputFormat.class);
        MockitoAnnotations.initMocks(this);
        this.partitionFileMergingDriver = spy(new PartitionFileMergingDriver(files));
    }

    @Test
    public void testRun() throws Exception {
        String[] args = new String[2];
        args[0] = "destinationPath";
        args[1] = comma;

        PowerMockito.whenNew(Path.class).withAnyArguments().thenReturn(path);
        when(path.getFileSystem(configuration)).thenReturn(fileSystem);
        when(partitionFileMergingDriver.getConf()).thenReturn(configuration);
        PowerMockito.when(SequenceFile.createWriter(any(FileSystem.class), any(), any(), any(), any(), any())).thenReturn(writer);
        doNothing().when(writer).append(any(LongWritable.class),any());
        doNothing().when(writer).close();
        PowerMockito.when(Job.getInstance(any(Configuration.class), any())).thenReturn(job);
        PowerMockito.doNothing().when(FileInputFormat.class, "addInputPath", any(), any());
        PowerMockito.doNothing().when(FileOutputFormat.class, "setOutputPath", any(), any());
        when(job.waitForCompletion(true)).thenReturn(true);

        int actual = partitionFileMergingDriver.run(args);
        assertEquals(actual, 0);
        verify(partitionFileMergingDriver, times(1)).getConf();
        verifyStatic(SequenceFile.class, times(1));
        SequenceFile.createWriter(any(FileSystem.class), any(), any(), any(), any(), any());
        verify(writer, times(1)).append(any(LongWritable.class), any());
        verify(writer, times(1)).close();
        verifyStatic(Job.class, times(1));
        Job.getInstance(any(Configuration.class), any());
        verifyStatic(FileInputFormat.class, times(1));
        FileInputFormat.addInputPath(any(), any());
        verifyStatic(FileOutputFormat.class, times(1));
        FileOutputFormat.setOutputPath(any(), any());
        verify(job, times(1)).waitForCompletion(true);
    }
}
