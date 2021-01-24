package com.flipkart.dsp.models.misc;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.flipkart.dsp.models.enums.WorkflowStateNotificationType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * +
 */
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class PartitionDetailsEmailNotificationRequest {
    private String logs;
    private Long requestId;
    private Long workflowId;
    private Map<String, Object> partitionDetails;
    private WorkflowStateNotificationType workflowStateNotificationType;
}
