package com.flipkart.dsp.response;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

/**
 * +
 */
@Data
@Builder
public class CreateWorkflowResponse implements Serializable {
    private long id;
    private String name;
    private double version;
}
