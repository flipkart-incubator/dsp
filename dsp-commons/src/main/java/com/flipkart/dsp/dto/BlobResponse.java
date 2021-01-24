package com.flipkart.dsp.dto;

import com.flipkart.dsp.entities.enums.BlobStatus;
import com.flipkart.dsp.entities.enums.BlobType;
import io.dropwizard.jackson.JsonSnakeCase;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.net.URI;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonSnakeCase
public class BlobResponse implements Serializable {

    private String requestId;
    private String location;
    private BlobType type;
    private BlobStatus status;


}
