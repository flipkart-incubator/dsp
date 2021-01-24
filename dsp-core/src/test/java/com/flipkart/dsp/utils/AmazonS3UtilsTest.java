package com.flipkart.dsp.utils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.flipkart.dsp.models.externalentities.CephEntity;
import com.flipkart.dsp.models.outputVariable.CephOutputLocation;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.flipkart.dsp.utils.Constants.SLEEP;
import static com.flipkart.dsp.utils.Constants.TRANSFER_RETRIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({AmazonS3Utils.class, RetryPolicies.class, Decryption.class, RetryProxy.class, AmazonS3Client.class, URL.class})
public class AmazonS3UtilsTest {

    private URL url;
    private CephEntity cephEntity;
    private String saltKey = "saltKey";
    private List<S3ObjectSummary> summaries = new ArrayList<>();

    @Mock private AmazonS3 amazonS3;
    @Mock private RetryPolicy retryPolicy;
    @Mock private ObjectListing objectListing;
    @Mock private AmazonS3Client amazonS3Client;
    @Mock private ObjectListing nextObjectListing;
    @Mock private S3ObjectSummary s3ObjectSummary;
    @Mock private CephOutputLocation cephOutputLocation;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        PowerMockito.mockStatic(URL.class);
        PowerMockito.mockStatic(Decryption.class);
        PowerMockito.mockStatic(RetryProxy.class);
        PowerMockito.mockStatic(RetryPolicies.class);
        PowerMockito.mockStatic(AmazonS3Client.class);

        url = PowerMockito.mock(URL.class);
        summaries.add(s3ObjectSummary);
        cephEntity = CephEntity.builder().host("host").secretKey("secretKey").accessKey("accessKey").build();

        PowerMockito.when(RetryPolicies.retryUpToMaximumCountWithFixedSleep(TRANSFER_RETRIES, SLEEP, TimeUnit.SECONDS))
                .thenReturn(retryPolicy);
        PowerMockito.when(Decryption.decrypt(cephEntity.getAccessKey(), saltKey)).thenReturn("accessKey");
        PowerMockito.when(Decryption.decrypt(cephEntity.getSecretKey(), saltKey)).thenReturn("secretKey");
        PowerMockito.whenNew(AmazonS3Client.class).withAnyArguments().thenReturn(amazonS3Client);
        PowerMockito.when(RetryProxy.create(AmazonS3.class, amazonS3Client, retryPolicy)).thenReturn(amazonS3);
    }

    @Test
    public void testGetAmazonS3() {
        AmazonS3 amazonS3 = AmazonS3Utils.getAmazonS3(saltKey, cephEntity);
        assertNotNull(amazonS3);
    }

    @Test
    public void testGetCephUrls() {
        Long requestId = 1L;
        String workflowName = "workflowName", dataFrameName = "dataFrameName";

        when(cephOutputLocation.getPath()).thenReturn("/ceph_path");
        when(cephOutputLocation.getBucket()).thenReturn("/ceph_bucket");
        when(amazonS3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(objectListing);
        when(objectListing.getObjectSummaries()).thenReturn(summaries);
        when(s3ObjectSummary.getKey()).thenReturn("key");
        when(amazonS3Client.listNextBatchOfObjects(objectListing)).thenReturn(nextObjectListing);
        when(nextObjectListing.getObjectSummaries()).thenReturn(new ArrayList<>());
        when(amazonS3Client.generatePresignedUrl(any())).thenReturn(url);

        List<URL> actual = AmazonS3Utils.getCephUrls(saltKey, requestId, workflowName, dataFrameName, cephEntity, cephOutputLocation);
        assertNotNull(actual);
        assertEquals(actual.size(), 1);

        verify(cephOutputLocation, times(1)).getPath();
        verify(cephOutputLocation, times(2)).getBucket();
        verify(amazonS3Client, times(1)).listObjects(any(ListObjectsRequest.class));
        verify(objectListing, times(1)).getObjectSummaries();
        verify(s3ObjectSummary, times(1)).getKey();
        verify(amazonS3Client, times(1)).listNextBatchOfObjects(objectListing);
        verify(nextObjectListing, times(1)).getObjectSummaries();
        verify(amazonS3Client, times(1)).generatePresignedUrl(any());
    }
}
