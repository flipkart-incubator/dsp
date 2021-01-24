package com.flipkart.dsp.entities.workflow;

import io.dropwizard.jackson.JsonSnakeCase;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 */

@Data
@Builder
@JsonSnakeCase
@NoArgsConstructor
@AllArgsConstructor
public class WorkflowMeta {

    private Long id;
    private String hiveQueue;
    private String mesosQueue;
    private String callbackUrl;
    private List<String> callbackEntities;
    private Long killTimeForNotification = 216000000L;
    private Long warningTimeForNotification = 10800000L;
}
