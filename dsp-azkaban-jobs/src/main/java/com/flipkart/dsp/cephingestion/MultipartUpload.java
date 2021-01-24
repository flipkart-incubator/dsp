package com.flipkart.dsp.cephingestion;

import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.flipkart.dsp.exceptions.CephIngestionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.concurrent.Callable;

/**
 */
@Slf4j
public class MultipartUpload implements Callable<PartETag> {
    private final int partNumber;
    private final File partFile;
    private final String md5sum;
    private final MultipartUploadOutputStream outputStream;

    MultipartUpload(MultipartUploadOutputStream outputStream, int partNumber, File partFile, String md5sum) {
        this.partNumber = partNumber;
        this.partFile = partFile;
        this.md5sum = md5sum;
        this.outputStream = outputStream;
    }

    /*
        Uploading a partFile to Ceph
     */
    public PartETag call() {
        UploadPartResult result;
        try (InputStream inputStream = new BufferedInputStream(new FileInputStream(this.partFile))) {
            UploadPartRequest request = new UploadPartRequest().withBucketName(outputStream.getBucketName())
                    .withKey(outputStream.getKey()).withUploadId(outputStream.getUploadId())
                    .withInputStream(inputStream).withPartNumber(partNumber).withPartSize(partFile.length()).withMD5Digest(md5sum);

            log.info("S3 uploadPart bucket:" + outputStream.getBucketName() + " key:" + outputStream.getKey() + " part:" + partNumber + " partFile:" + partFile);
            result = outputStream.getAmazonS3().uploadPart(request);
            FileUtils.forceDelete(this.partFile);
        } catch (Exception e) {
            this.partFile.delete();
            throw new CephIngestionException(String.format("Exception in Uploading part %s: error %s", partNumber, e.getMessage()));
        }
        return result.getPartETag();
    }
}
