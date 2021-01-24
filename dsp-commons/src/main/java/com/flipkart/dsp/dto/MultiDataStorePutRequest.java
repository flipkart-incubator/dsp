package com.flipkart.dsp.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Duration;

@Data
@AllArgsConstructor
public class MultiDataStorePutRequest {
    private Payload payload;
    private Duration timeToLive; /*in milliseconds*/
    private String namespace;
    private MultiDataStoreStorageAdapter storageAdapter;
}
