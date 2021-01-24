package com.flipkart.dsp.sg.helper;

import com.flipkart.dsp.qe.entity.HiveConfigParam;
import com.flipkart.dsp.sg.exceptions.DistCpException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ToolRunner.class, DistCp.class, DistCpOptions.class, HadoopDistributedCopy.class})
public class HadoopDistributedCopyTest {

    @Mock private DistCp distCp;
    @Mock private DistCpOptions distCpOptions;
    @Mock private Configuration configuration;
    @Mock private HiveConfigParam hiveConfigParam;
    private HadoopDistributedCopy hadoopDistributedCopy;

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(ToolRunner.class);
        MockitoAnnotations.initMocks(this);
        this.hadoopDistributedCopy = spy(new HadoopDistributedCopy(hiveConfigParam));
        when(hiveConfigParam.getRetryGapInMillis()).thenReturn(10);
        PowerMockito.whenNew(DistCpOptions.class).withAnyArguments().thenReturn(distCpOptions);
        PowerMockito.whenNew(DistCp.class).withAnyArguments().thenReturn(distCp);
        PowerMockito.when(ToolRunner.run(any(), any())).thenReturn(0);
    }

    @Test
    public void testRunSuccess() throws Exception {
        hadoopDistributedCopy.run(configuration, "source", "destination", true, 30, "destBasePath");
        verify(hiveConfigParam, times(1)).getRetryGapInMillis();
        PowerMockito.verifyNew(DistCpOptions.class).withArguments(any(), any());
        PowerMockito.verifyNew(DistCp.class).withArguments(any(), any());
        PowerMockito.verifyStatic(ToolRunner.class);
        ToolRunner.run(any(), any());
    }

    @Test
    public void testRunFailureCase1() throws Exception {
        when(hiveConfigParam.getMaxRetries()).thenReturn(1);
        PowerMockito.when(ToolRunner.run(any(), any())).thenReturn(1);
        boolean isException = false;
        try {
            hadoopDistributedCopy.run(configuration, "source", "destination", true, 30,"destBasePath");
        } catch (DistCpException e) {
            isException = true;
            assertTrue(e.getMessage().contains("Distributed Copy action failed for: source:"));
        }

        assertTrue(isException);
        verify(hiveConfigParam, times(1)).getRetryGapInMillis();
        PowerMockito.verifyNew(DistCpOptions.class).withArguments(any(), any());
        PowerMockito.verifyNew(DistCp.class).withArguments(any(), any());
        PowerMockito.verifyStatic(ToolRunner.class);
        ToolRunner.run(any(), any());
    }
}
