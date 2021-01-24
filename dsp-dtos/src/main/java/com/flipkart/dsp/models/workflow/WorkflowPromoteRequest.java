package com.flipkart.dsp.models.workflow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.dsp.models.misc.EmailNotifications;
import io.dropwizard.jackson.JsonSnakeCase;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotNull;

import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * +
 */
@Builder
@Data
@NoArgsConstructor
@JsonSnakeCase
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(NON_NULL)
public class WorkflowPromoteRequest {
    @NotNull
    @JsonProperty("commit_id")
    String commitId;

    @NotNull
    @JsonProperty("mesos_queue")
    String mesosQueue;

    @NotNull
    @JsonProperty("hive_queue")
    String hiveQueue;

    @JsonProperty("email_notifications")
    EmailNotifications emailNotifications;
}
