package com.flipkart.dsp.dto.trace;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class SignalPojo {
    private String name;
    private String dataType;
    private SignalColumnPojo fact;
}
