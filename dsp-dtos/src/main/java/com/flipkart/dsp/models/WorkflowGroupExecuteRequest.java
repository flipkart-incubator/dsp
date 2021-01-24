package com.flipkart.dsp.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.dsp.models.misc.EmailNotifications;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(NON_NULL)
public class WorkflowGroupExecuteRequest {
    @JsonProperty("request_id")
    Long requestId;

    @JsonProperty("test_run")
    Boolean testRun = false;

    @JsonProperty("call_back_url")
    String callBackUrl;

    Map<String/*hive table*/, Long> tables;

    Map<String/*workflowName*/, RequestOverride> overrides;

    @JsonProperty("email_notifications")
    EmailNotifications emailNotifications;
}
