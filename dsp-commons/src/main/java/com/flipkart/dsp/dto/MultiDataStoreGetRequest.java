package com.flipkart.dsp.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class MultiDataStoreGetRequest {
    private String payloadId;
    private MultiDataStoreStorageAdapter storageAdapter;
    private MultiDataStorePayloadFormat payloadFormat;
    private String payloadFileLocation;
}
