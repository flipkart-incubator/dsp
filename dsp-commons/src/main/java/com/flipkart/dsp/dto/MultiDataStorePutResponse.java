package com.flipkart.dsp.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

import java.util.LinkedHashMap;

@Data
@AllArgsConstructor
@ToString
public class MultiDataStorePutResponse {
    private String payloadId;
    private MultiDataStoreStorageAdapter storageAdapter;

    public static MultiDataStorePutResponse valueOf(Object object) {
        return object ==null ? null : (MultiDataStorePutResponse) object;
    }

    public static MultiDataStorePutResponse from(LinkedHashMap<String,String> object) {
        return new MultiDataStorePutResponse(object.get("payloadId"), MultiDataStoreStorageAdapter.valueOf(object.get("storageAdapter")));
    }
}
