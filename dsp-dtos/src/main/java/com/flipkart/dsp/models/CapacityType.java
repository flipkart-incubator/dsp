package com.flipkart.dsp.models;

import lombok.Getter;

import static java.util.Arrays.asList;

@Getter
public enum CapacityType {
    S(1, 8), M(2, 16), L(4, 32), XL(8, 64), XXL(12, 96);

    private int cpu;
    private int memory;

    CapacityType(int cpu, int memoryInGB) {
        this.cpu = cpu;
        this.memory = memoryInGB;
    }

    public static CapacityType byCpu(final int cpu) {
        return asList(CapacityType.values()).stream().filter(v -> v.cpu == cpu).findFirst().get();
    }

    public static CapacityType byMemory(final int memoryInGB) {
        return asList(CapacityType.values()).stream().filter(v -> v.memory == memoryInGB).findFirst().get();
    }
}
