package com.flipkart.dsp.client;

import com.flipkart.dsp.dto.*;
import com.flipkart.dsp.exceptions.HDFSUtilsException;
import com.flipkart.dsp.exceptions.MultiDataStoreClientException;
import com.flipkart.dsp.exceptions.StorageAdapterNotFoundException;
import com.flipkart.dsp.utils.HdfsUtils;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;

import java.io.IOException;

@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class MultiDatastoreClient {
    private final HdfsUtils hdfsUtils;
    private String FOLDER_SEPARATOR = "/";

    public MultiDataStoreGetResponse get(MultiDataStoreGetRequest getRequest) throws MultiDataStoreClientException {
        try {
            switch (getRequest.getStorageAdapter()) {
                case HDFS:
                    if (getRequest.getPayloadFormat() == MultiDataStorePayloadFormat.FILE) {
                        final String remoteFilePath = getRequest.getPayloadId();
                        final String localDirectoryPath = getRequest.getPayloadFileLocation();
                        hdfsUtils.copyFromHdfsToLocal(remoteFilePath, localDirectoryPath);
                        String localFileName = getFileName(remoteFilePath);
                        return new MultiDataStoreGetResponse(new FilePayload(localDirectoryPath + FOLDER_SEPARATOR + localFileName));
                    } else {
                        //todo: implement when needed
                    }
                default:
                    throw new StorageAdapterNotFoundException("Storage adapter:" + getRequest.getStorageAdapter() + " not found!");
            }
        } catch (IOException | StorageAdapterNotFoundException e) {
            throw new MultiDataStoreClientException("Failed to get the resource requested because of following reason", e);
        }
    }

    private String getFileName(String remoteFilePath) {
        return remoteFilePath.split(FOLDER_SEPARATOR)[remoteFilePath.split(FOLDER_SEPARATOR).length - 1];
    }

    public MultiDataStorePutResponse put(MultiDataStorePutRequest putRequest) throws MultiDataStoreClientException {
        try {
            switch (putRequest.getStorageAdapter()) {
                case HDFS:
                    if (putRequest.getPayload() instanceof FilePayload) {
                        String localFilePath = ((FilePayload) putRequest.getPayload()).getContent();
                        String remoteLocation = putRequest.getNamespace();
                        hdfsUtils.copyFromLocalToHdfs(localFilePath, remoteLocation);
                        String localFileName = getFileName(localFilePath);
                        final String remoteFilePath = putRequest.getNamespace() + FOLDER_SEPARATOR + localFileName;
                        return new MultiDataStorePutResponse(remoteFilePath, MultiDataStoreStorageAdapter.HDFS);
                    } else {
                        //todo: implement when needed
                    }
                default:
                    throw new StorageAdapterNotFoundException("Storage adapter:" + putRequest.getStorageAdapter() + " not found!");
            }
        } catch (IOException | HDFSUtilsException | StorageAdapterNotFoundException e) {
            throw new MultiDataStoreClientException("Failed to get the resource requested because of following reason", e);
        }
    }
}
