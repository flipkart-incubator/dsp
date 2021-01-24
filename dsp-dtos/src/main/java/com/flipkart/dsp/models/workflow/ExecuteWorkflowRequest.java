package com.flipkart.dsp.models.workflow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.flipkart.dsp.models.RequestOverride;
import com.flipkart.dsp.models.misc.EmailNotifications;
import io.dropwizard.jackson.JsonSnakeCase;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonSnakeCase
@JsonInclude(NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class ExecuteWorkflowRequest {
    private Boolean isProd;
    private Long requestId;
    private String callBackUrl;
    private String workflowName;
    private String workflowVersion;
    private Boolean testRun = false;
    private Long parentWorkflowRefreshId;
    private RequestOverride requestOverride;
    private EmailNotifications emailNotifications;
    private Map<String/*hive table*/, Long> tables;
}
