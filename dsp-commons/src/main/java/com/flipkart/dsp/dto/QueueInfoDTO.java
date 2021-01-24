package com.flipkart.dsp.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class QueueInfoDTO implements Serializable {

    @JsonProperty("hive_queue")
    private String hiveQueue;

    @JsonProperty("mesos_queue")
    private String mesosQueue;
}
