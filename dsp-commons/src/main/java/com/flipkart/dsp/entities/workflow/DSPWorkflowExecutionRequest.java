package com.flipkart.dsp.entities.workflow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.dsp.models.Label;
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
public class DSPWorkflowExecutionRequest implements Serializable  {
    @JsonProperty("request_id")
    Long requestId;

    @JsonProperty("partition_overrides")
    Map<@Label("entityName") String, @Label("partitionId") Long> partitionOverrides;

    @JsonProperty("table_overrides")
    Map<@Label("tableName") String, @Label("tempTableName") String> tableOverrides;

    @JsonProperty("callback_url")
    String callBackUrl;
}
