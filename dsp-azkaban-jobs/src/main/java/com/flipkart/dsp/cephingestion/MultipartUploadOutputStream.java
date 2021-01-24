package com.flipkart.dsp.cephingestion;


import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.exceptions.CephIngestionException;
import com.flipkart.dsp.models.externalentities.CephEntity;
import com.flipkart.dsp.utils.AmazonS3Utils;
import com.flipkart.dsp.utils.Constants;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.nio.charset.Charset;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.flipkart.dsp.utils.Constants.*;


@Getter
@Slf4j
public class MultipartUploadOutputStream extends OutputStream implements AutoCloseable {

    private String key;
    private File tempDir;
    private Long requestId;
    private String uploadId;
    private File currentTemp;
    private int partCount = 0;
    private String bucketName;
    private AmazonS3 amazonS3;
    private long currentPartSize = 0L;
    private DigestOutputStream currentOutput;
    private ObjectMapper objectMapper = new ObjectMapper();
    private List<Future<PartETag>> futures = new ArrayList<>();
    private ExecutorService threadPool = Executors.newFixedThreadPool(10);

    public MultipartUploadOutputStream(String cephKey, String saltKey, String bucketName, CephEntity cephEntity, Long requestId) throws CephIngestionException {
        this.key = cephKey;
        this.requestId = requestId;
        this.tempDir = getTempDir();
        this.bucketName = bucketName;

        amazonS3 = AmazonS3Utils.getAmazonS3(saltKey, cephEntity);
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucketName, this.getKey());
        InitiateMultipartUploadResult result = amazonS3.initiateMultipartUpload(request);
        uploadId = result.getUploadId();
        setTempFileAndOutput();
    }

    private File getTempDir() {
        String[] backupDirs = new Configuration().get("fs.s3.buffer.dir").split(comma);
        File dir = new File(backupDirs[0]+slash+ Constants.REFRESH_ID +equal+requestId);
        dir.mkdirs();
        return dir;
    }
    private void setTempFileAndOutput() throws CephIngestionException {
        try {
            currentPartSize = 0L;
            currentTemp = new File(tempDir, "multipart-" + this.getUploadId() + "-" + partCount++);
            this.currentOutput = new DigestOutputStream(new BufferedOutputStream(new FileOutputStream(currentTemp)), MessageDigest.getInstance("MD5"));
        } catch (FileNotFoundException | NoSuchAlgorithmException e) {
            String errorMessage = String.format("Not able to create temp File for MultiPartUpload, ErrorMessage: %s", e.getMessage());
            throw new CephIngestionException(errorMessage, e.getCause());
        }
    }

    public void write(byte[] b, int off, int len) throws IOException {
        long capacityLeft = capacityLeft();
        int offset = off;
        int length = len;
        while (capacityLeft < length) {
            int capacityLeftInt = (int) capacityLeft;
            this.getCurrentOutput().write(b, offset, capacityLeftInt);
            kickOffUpload();
            offset += capacityLeftInt;
            length -= capacityLeftInt;
            capacityLeft = capacityLeft();
        }
        this.getCurrentOutput().write(b, offset, length);
        currentPartSize += length;
    }

    public void write(int b) {}

    public void close() {
        try {
            kickOffUpload();
            boolean anyNotDone = false;
            while (!anyNotDone) {
                anyNotDone = true;
                for (Future future : futures) {
                    anyNotDone &= future.isDone();
                }
                Thread.sleep(1000L);
            }

            List<PartETag> etags = new ArrayList<>();
            for (Future future : futures) {
                etags.add((PartETag) future.get());
            }
            this.getAmazonS3().completeMultipartUpload(new CompleteMultipartUploadRequest(this.getBucketName(), key, uploadId, etags));
            FileUtils.forceDelete(this.getCurrentTemp());
        } catch (Exception e) {
            this.getAmazonS3().abortMultipartUpload(new AbortMultipartUploadRequest(this.getBucketName(), key, uploadId));
            String errorMessage = String.format("Error In completing multipart upload, aborting Upload. ErrorMessage: %s", e.getMessage());
            throw new CephIngestionException(errorMessage, e.getCause());
        } finally {
            threadPool.shutdown();
        }
    }

    private void kickOffUpload() throws IOException {
        this.getCurrentOutput().close();
        String md5sum = new String(Base64.encodeBase64(this.currentOutput.getMessageDigest().digest()), Charset.forName("UTF-8"));
        Callable<PartETag> callable = new MultipartUpload(this, partCount, this.getCurrentTemp(), md5sum);
        futures.add(threadPool.submit(callable));
        setTempFileAndOutput();
    }

    private long capacityLeft() {
        return MULTIPART_SIZE - currentPartSize;
    }
}
