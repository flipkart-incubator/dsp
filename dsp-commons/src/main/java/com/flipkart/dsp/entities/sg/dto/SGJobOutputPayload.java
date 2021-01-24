package com.flipkart.dsp.entities.sg.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.util.Set;

/**
 */

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class SGJobOutputPayload {

    @JsonProperty("payload_set")
    private Set<SGUseCasePayload> sgUseCasePayloadSet;
}
