package com.flipkart.dsp.entities.misc;

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
public class EntityIngestedNotificationResponse implements Serializable {
    @JsonProperty("entities")
    Map<@Label("entityName") String, IngestionAttributes> entities;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class IngestionAttributes implements Serializable {
        @JsonProperty("partition_id")
        private String partitionId;

        @JsonProperty("comments")
        private String comments;
    }
}
