package com.flipkart.dsp.entities.subscription;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.dsp.entities.enums.SubscriptionRunStatusEnum;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * +
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown=true)
public class SubscriptionCallback implements Serializable {
    @JsonProperty("subscription_id")
    private String subscriptionId;
    @JsonProperty("subscription_run_id")
    private Long subscriptionRunId;
    @JsonProperty("subscription_run_status")
    private SubscriptionRunStatusEnum subscriptionRunStatus;
    @JsonProperty("message")
    private String message;
    @JsonProperty("tables")
    private Map<String, Long> tablesPartitions;
}
