package com.flipkart.dsp.sg.helper;

import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.models.sg.Signal;
import com.flipkart.dsp.models.sg.SignalGroup;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * +
 */
public class PartitionHelperTest {
    @Mock private Signal signal;
    @Mock private DataFrame dataFrame;
    @Mock private SignalGroup signalGroup;
    @Mock private SignalGroup.SignalMeta signalMeta;

    private PartitionHelper partitionHelper;
    private String signalName = "signalName";
    private List<String> partitions = new ArrayList<>();
    private List<SignalGroup.SignalMeta> signalMetas = new ArrayList<>();

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        this.partitionHelper = spy(new PartitionHelper());

        signalMetas.add(signalMeta);
        partitions.add(signalName);
        partitions.add(signalName + "_1");
    }

    @Test
    public void testGetPartitionForDataframe() {
        when(dataFrame.getPartitions()).thenReturn(partitions);
        when(dataFrame.getSignalGroup()).thenReturn(signalGroup);
        when(signalGroup.getSignalMetas()).thenReturn(signalMetas);
        when(signalMeta.getSignal()).thenReturn(signal);
        when(signal.getName()).thenReturn(signalName);

        LinkedHashSet<Signal> expected = partitionHelper.getPartitionForDataframe(dataFrame);
        assertNotNull(expected);
        assertEquals(expected.size(), 1);
        verify(dataFrame).getPartitions();
        verify(dataFrame).getSignalGroup();
        verify(signalGroup).getSignalMetas();
        verify(signalMeta, times(3)).getSignal();
        verify(signal, times(2)).getName();
    }

    @Test
    public void testDoesPartitionMatches() {
        Set<String> requiredPartition = new HashSet<>();
        Set<String> existingPartition = new HashSet<>();
        assertTrue(partitionHelper.doesPartitionMatches(requiredPartition, existingPartition));

        requiredPartition.add(signalName + "_1");
        requiredPartition.add(signalName);
        assertFalse(partitionHelper.doesPartitionMatches(requiredPartition, existingPartition));

        existingPartition.add(signalName);
        existingPartition.add(signalName + "_1");
        assertTrue(partitionHelper.doesPartitionMatches(requiredPartition, existingPartition));
    }

}
