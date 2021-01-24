package com.flipkart.dsp.utils;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import com.flipkart.dsp.models.externalentities.CephEntity;
import com.flipkart.dsp.models.outputVariable.CephOutputLocation;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.retry.*;

import java.net.URL;
import java.sql.Date;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.flipkart.dsp.utils.Constants.*;

/**
 * +
 */
@Slf4j
public class AmazonS3Utils {

    public static AmazonS3 getAmazonS3(String saltKey, CephEntity cephEntity) {
        RetryPolicy retryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(TRANSFER_RETRIES, SLEEP, TimeUnit.SECONDS);
        AmazonS3Client amazonS3Client = getAmazonS3Client(saltKey, cephEntity);
        return ((AmazonS3) RetryProxy.create(AmazonS3.class, amazonS3Client, retryPolicy));
    }

    private static AmazonS3Client getAmazonS3Client(String saltKey, CephEntity cephEntity) {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setProtocol(Protocol.HTTP);
        AWSCredentials credentials = new BasicAWSCredentials(
                Decryption.decrypt(cephEntity.getAccessKey(), saltKey),
                Decryption.decrypt(cephEntity.getSecretKey(), saltKey));
        AmazonS3Client amazonS3Client = new AmazonS3Client(credentials, clientConfiguration);
        amazonS3Client.setEndpoint(cephEntity.getHost());
        amazonS3Client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
        return amazonS3Client;
    }

    public static List<URL> getCephUrls(String saltKey, Long requestId, String workflowName, String dataFrameName,
                                       CephEntity cephEntity, CephOutputLocation cephOutputLocation) {
        List<URL> urls = new ArrayList<>();
        AmazonS3Client amazonS3Client = AmazonS3Utils.getAmazonS3Client(saltKey, cephEntity);
        String cephKey = getCephkey(requestId, cephOutputLocation.getPath(), workflowName, dataFrameName);
        List<String> keys = getObjectListFromFolder(cephKey, cephOutputLocation, amazonS3Client);
        for (String key : keys) {
            GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(cephOutputLocation.getBucket(), key);
            request.setExpiration(Date.from(LocalDate.now().plusMonths(3).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant()));
            urls.add(amazonS3Client.generatePresignedUrl(request));
        }
        return urls;
    }

    public static String getCephkey(Long requestId, String path, String workflowName, String dataFrameName) {
        return path + slash + workflowName + equal + requestId.toString() + slash + dataFrameName;
    }

    private static List<String> getObjectListFromFolder(String cephKey, CephOutputLocation cephOutputLocation, AmazonS3Client amazonS3Client) {
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(cephOutputLocation.getBucket()).withPrefix(cephKey + slash);

        List<String> keys = new ArrayList<>();
        ObjectListing objects = amazonS3Client.listObjects(listObjectsRequest);
        while (true) {
            List<S3ObjectSummary> summaries = objects.getObjectSummaries();
            if (summaries.size() < 1)
                break;
            summaries.forEach(s -> keys.add(s.getKey()));
            objects = amazonS3Client.listNextBatchOfObjects(objects);
        }
        return keys;
    }
}
