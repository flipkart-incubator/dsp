package com.flipkart.dsp.sg.helper;

import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.models.sg.Signal;
import com.flipkart.dsp.models.sg.SignalGroup;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PartitionHelper {

    public LinkedHashSet<Signal> getPartitionForDataframe(DataFrame dataframe) {
        List<String> pipelineStepPartitions = dataframe.getPartitions();
        final Map<String, Signal> signalMap = dataframe.getSignalGroup().getSignalMetas().stream().filter(signalMeta
                -> pipelineStepPartitions.contains(signalMeta.getSignal().getName()))
                .collect(Collectors.toMap(sm -> sm.getSignal().getName(), SignalGroup.SignalMeta::getSignal));
        LinkedHashSet<Signal> signalSet = new LinkedHashSet<>();
        pipelineStepPartitions.forEach(wp -> {
            if (signalMap.containsKey(wp)) {
                signalSet.add(signalMap.get(wp));
            }
        });
        return signalSet;
    }

    boolean doesPartitionMatches(Set<String> requiredPartition, Set<String> existingPartition) {
        return (requiredPartition.size() == existingPartition.size() && requiredPartition.containsAll(existingPartition));
    }
}
