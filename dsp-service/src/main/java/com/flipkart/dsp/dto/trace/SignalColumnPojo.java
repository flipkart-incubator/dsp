package com.flipkart.dsp.dto.trace;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class SignalColumnPojo {
    private String table;
    private String column;
}
