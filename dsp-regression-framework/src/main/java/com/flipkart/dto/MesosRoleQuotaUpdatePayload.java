package com.flipkart.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * +
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MesosRoleQuotaUpdatePayload {
    private boolean force;
    private String role;

    @JsonProperty("guarantee")
    private List<ResourceInformation> resourceInformation;

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ResourceInformation {
        private String name;
        private String type;
        private Scalar scalar;
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Scalar {
        private Long value;
    }
}
