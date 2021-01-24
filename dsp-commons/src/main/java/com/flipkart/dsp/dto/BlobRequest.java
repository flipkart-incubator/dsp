package com.flipkart.dsp.dto;

import com.flipkart.dsp.entities.enums.BlobStatus;
import com.flipkart.dsp.entities.enums.BlobType;
import io.dropwizard.jackson.JsonSnakeCase;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.validator.constraints.Length;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonSnakeCase
public class BlobRequest implements Serializable {

    @Length(max = 50)
    private String requestId;
    private BlobType type;
    private BlobStatus status;

}
