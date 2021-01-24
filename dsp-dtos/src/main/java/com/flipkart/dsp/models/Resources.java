package com.flipkart.dsp.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Resources {

    @Valid
    @NotNull
    private CapacityType cpu;

    @Valid
    @NotNull
    private CapacityType memory;
}
