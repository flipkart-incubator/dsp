package com.flipkart.dsp.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ExecutionOutput {

    @JsonProperty("job_id")
    Long jobId;

    @JsonProperty("azkaban_url")
    String azkabanUrl;

    @JsonProperty
    Set<Event> messageList;
}
