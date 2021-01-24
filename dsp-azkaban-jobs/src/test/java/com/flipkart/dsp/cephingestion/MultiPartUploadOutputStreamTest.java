package com.flipkart.dsp.cephingestion;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.flipkart.dsp.exceptions.CephIngestionException;
import com.flipkart.dsp.models.externalentities.CephEntity;
import com.flipkart.dsp.utils.AmazonS3Utils;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static junit.framework.TestCase.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({MultipartUploadOutputStream.class, AmazonS3Utils.class, BufferedInputStream.class, FileUtils.class,
        FileInputStream.class, DigestOutputStream.class, MultipartUpload.class, Executors.class, MessageDigest.class})
public class MultiPartUploadOutputStreamTest {
    @Mock private PartETag partETag;
    @Mock private AmazonS3 amazonS3;
    @Mock private CephEntity cephEntity;
    @Mock private MessageDigest messageDigest;
    @Mock private MultipartUpload multipartUpload;
    @Mock private ExecutorService executorService;
    @Mock private FileOutputStream fileOutputStream;
    @Mock private Future<PartETag> partETagFuture;
    @Mock private DigestOutputStream digestOutputStream;
    @Mock private BufferedOutputStream bufferedOutputStream;
    @Mock private InitiateMultipartUploadResult multipartUploadResult;
    @Mock private CompleteMultipartUploadResult completeMultipartUploadResult;

    private String saltKey = "saltKey";
    private Long requestId = Long.valueOf(12345678);
    private MultipartUploadOutputStream multipartUploadOutputStream;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        PowerMockito.mockStatic(Executors.class);
        PowerMockito.mockStatic(FileUtils.class);
        PowerMockito.mockStatic(MessageDigest.class);
        PowerMockito.mockStatic(AmazonS3Utils.class);
        PowerMockito.mockStatic(FileOutputStream.class);
        PowerMockito.mockStatic(MultipartUpload.class);
        PowerMockito.mockStatic(DigestOutputStream.class);
        PowerMockito.mockStatic(BufferedOutputStream.class);

        String cephKey = "cephKey";
        String bucket = "ceph_bucket";

        doNothing().when(digestOutputStream).close();
        when(messageDigest.digest()).thenReturn(new byte[200]);
        when(multipartUploadResult.getUploadId()).thenReturn("upload_id");
        when(digestOutputStream.getMessageDigest()).thenReturn(messageDigest);
        when(executorService.submit(any(Callable.class))).thenReturn(partETagFuture);
        when(amazonS3.initiateMultipartUpload(any())).thenReturn(multipartUploadResult);
        PowerMockito.when(AmazonS3Utils.getAmazonS3(saltKey, cephEntity)).thenReturn(amazonS3);
        PowerMockito.when(Executors.newFixedThreadPool(10)).thenReturn(executorService);
        PowerMockito.whenNew(FileOutputStream.class).withAnyArguments().thenReturn(fileOutputStream);
        PowerMockito.whenNew(MultipartUpload.class).withAnyArguments().thenReturn(multipartUpload);
        PowerMockito.whenNew(DigestOutputStream.class).withAnyArguments().thenReturn(digestOutputStream);
        PowerMockito.whenNew(BufferedOutputStream.class).withAnyArguments().thenReturn(bufferedOutputStream);

        multipartUploadOutputStream = new MultipartUploadOutputStream(cephKey, saltKey, bucket, cephEntity, requestId);
    }

    @Test
    public void testWriteSuccess() throws Exception {
        doNothing().when(digestOutputStream).write(any(), anyInt(), anyInt());
        multipartUploadOutputStream.write(new byte[100], 0, 20971530);

        verify(multipartUploadResult, times(1)).getUploadId();
        verify(amazonS3, times(1)).initiateMultipartUpload(any());
        verify(digestOutputStream, times(2)).write(any(), anyInt(), anyInt());
        verify(digestOutputStream, times(1)).close();
        verify(digestOutputStream, times(1)).getMessageDigest();
        verify(messageDigest, times(1)).digest();
        verify(executorService, times(1)).submit(any(Callable.class));
        verifyStatic(AmazonS3Utils.class);
        AmazonS3Utils.getAmazonS3(saltKey, cephEntity);
        verifyStatic(Executors.class);
        Executors.newFixedThreadPool(10);
        verifyNew(FileOutputStream.class, times(2)).withArguments(any());
        verifyNew(MultipartUpload.class).withArguments(any(),any(), any(), any());
        verifyNew(BufferedOutputStream.class,times(2)).withArguments(any());
        verifyNew(DigestOutputStream.class,times(2)).withArguments(any(), any());
    }

    @Test
    public void testWriteFailure() throws Exception {
        PowerMockito.when(MessageDigest.getInstance(any())).thenThrow(new NoSuchAlgorithmException());

        boolean isException = false;
        try {
            multipartUploadOutputStream.write(new byte[100], 0, 20971530);
        } catch (CephIngestionException e) {
            isException = true;
            assertTrue(e.getMessage().contains("Not able to create temp File for MultiPartUpload, ErrorMessage: "));
        }

        assertTrue(isException);
        verifyStatic(AmazonS3Utils.class);
        AmazonS3Utils.getAmazonS3(saltKey, cephEntity);
        verifyStatic(Executors.class);
        Executors.newFixedThreadPool(10);
        verifyNew(FileOutputStream.class, times(2)).withArguments(any());
        verifyNew(MultipartUpload.class).withArguments(any(),any(), any(), any());
        verifyNew(BufferedOutputStream.class,times(2)).withArguments(any());
        verifyNew(DigestOutputStream.class,times(1)).withArguments(any(), any());

    }

    @Test
    public void testCloseSuccess() throws Exception {
        PowerMockito.doNothing().when(FileUtils.class, "forceDelete", any());
        when(partETagFuture.isDone()).thenReturn(true);
        when(partETagFuture.get()).thenReturn(partETag);
        when(amazonS3.completeMultipartUpload(any())).thenReturn(completeMultipartUploadResult);
        doNothing().when(executorService).shutdown();
        multipartUploadOutputStream.close();

        verify(digestOutputStream, times(1)).close();
        verify(digestOutputStream, times(1)).getMessageDigest();
        verify(messageDigest, times(1)).digest();
        verify(partETagFuture, times(1)).isDone();
        verify(partETagFuture, times(1)).get();
        verify(amazonS3, times(1)).completeMultipartUpload(any());
        verify(executorService, times(1)).shutdown();
        verifyStatic(AmazonS3Utils.class);
        AmazonS3Utils.getAmazonS3(saltKey, cephEntity);
        verifyStatic(Executors.class);
        Executors.newFixedThreadPool(10);
        verifyNew(FileOutputStream.class, times(2)).withArguments(any());
        verifyNew(MultipartUpload.class).withArguments(any(),any(), any(), any());
        verifyNew(BufferedOutputStream.class,times(2)).withArguments(any());
        verifyNew(DigestOutputStream.class,times(2)).withArguments(any(), any());

    }

    @Test
    public void testCloseFailure() throws Exception {
        boolean isException = false;
        when(partETagFuture.isDone()).thenReturn(true);
        when(partETagFuture.get()).thenReturn(partETag);
        when(amazonS3.completeMultipartUpload(any())).thenThrow(new AmazonServiceException("Error"));
        doNothing().when(amazonS3).abortMultipartUpload(any());
        doNothing().when(executorService).shutdown();

        try {
            multipartUploadOutputStream.close();
        } catch (Exception e) {
            isException = true;
            assertTrue(e.getMessage().contains("Error In completing multipart upload, aborting Upload."));
        }

        assertTrue(isException);
        verify(digestOutputStream, times(1)).close();
        verify(digestOutputStream, times(1)).getMessageDigest();
        verify(messageDigest, times(1)).digest();
        verify(partETagFuture, times(1)).isDone();
        verify(partETagFuture, times(1)).get();
        verify(amazonS3, times(1)).completeMultipartUpload(any());
        verify(amazonS3, times(1)).abortMultipartUpload(any());
        verify(executorService, times(1)).shutdown();
        verifyStatic(AmazonS3Utils.class);
        AmazonS3Utils.getAmazonS3(saltKey, cephEntity);
        verifyStatic(Executors.class);
        Executors.newFixedThreadPool(10);
        verifyNew(FileOutputStream.class, times(2)).withArguments(any());
        verifyNew(MultipartUpload.class).withArguments(any(),any(), any(), any());
        verifyNew(BufferedOutputStream.class,times(2)).withArguments(any());
        verifyNew(DigestOutputStream.class,times(2)).withArguments(any(), any());
    }

    @Test
    public void testWrite() throws Exception  {
        multipartUploadOutputStream.write(10);
        verifyStatic(AmazonS3Utils.class);
        AmazonS3Utils.getAmazonS3(saltKey, cephEntity);
        verifyStatic(Executors.class);
        Executors.newFixedThreadPool(10);
        verifyNew(FileOutputStream.class, times(1)).withArguments(any());
        verifyNew(BufferedOutputStream.class,times(1)).withArguments(any());
        verifyNew(DigestOutputStream.class,times(1)).withArguments(any(), any());
    }
}
