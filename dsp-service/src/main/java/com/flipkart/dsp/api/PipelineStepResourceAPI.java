package com.flipkart.dsp.api;

import com.flipkart.dsp.entities.pipelinestep.PipelineStepResources;
import com.flipkart.dsp.models.CapacityType;
import com.flipkart.dsp.models.Resources;

import java.util.Arrays;
import java.util.List;

import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toList;

/**
 * +
 */
class PipelineStepResourceAPI {

    PipelineStepResources preparePipelineStepResource(Resources resources) {
        if (isNull(resources) || isNull(resources.getCpu()) || isNull(resources.getMemory()))
            return new PipelineStepResources();

        double cpu = resources.getCpu().getCpu();
        long memoryInMB = resources.getMemory().getMemory() * 1024;
        return PipelineStepResources.builder().baseCpu(cpu).baseMemory(memoryInMB)
                .trainingCpuCoefficient(0.0).trainingMemoryCoefficient(0.0)
                .executionCpuCoefficient(0.0).executionMemoryCoefficient(0.0).build();
    }

    Resources wrapResources(PipelineStepResources pipelineStepResources) {
        int cpuRounded = roundOffCPU(pipelineStepResources.getBaseCpu().intValue());
        CapacityType cpu = CapacityType.byCpu(cpuRounded);

        double memoryInGB = pipelineStepResources.getBaseMemory() / 1024.0;
        int memoryInGBRounded = roundOffMemory((int) memoryInGB);
        CapacityType memory = CapacityType.byMemory(memoryInGBRounded);
        return Resources.builder().cpu(cpu).memory(memory).build();
    }

    private int roundOffCPU(int num) {
        List<Integer> availableCPUs = Arrays.stream(CapacityType.values()).map(CapacityType::getCpu).collect(toList());
        return roundOff(num, availableCPUs);
    }

    private int roundOffMemory(int num) {
        List<Integer> availableMemories = Arrays.stream(CapacityType.values()).map(CapacityType::getMemory).collect(toList());
        return roundOff(num, availableMemories);
    }

    private int roundOff(int num, List<Integer> constants) {
        if (constants.contains(num))
            return num;

        for (int constant : constants) {
            if (num < constant)
                return constant;
        }
        return constants.get(constants.size() - 1);
    }
}
