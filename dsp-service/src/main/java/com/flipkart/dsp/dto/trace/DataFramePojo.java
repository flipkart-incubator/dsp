package com.flipkart.dsp.dto.trace;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class DataFramePojo {
    private RequestType type;
    private String name;
}
