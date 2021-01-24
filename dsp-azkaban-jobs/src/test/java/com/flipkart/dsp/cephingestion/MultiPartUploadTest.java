package com.flipkart.dsp.cephingestion;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.flipkart.dsp.exceptions.CephIngestionException;
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
import java.io.File;
import java.io.FileInputStream;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.verifyNew;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({MultipartUpload.class, BufferedInputStream.class, FileUtils.class, FileInputStream.class})
public class MultiPartUploadTest {

    @Mock private File partFile;
    @Mock private PartETag partETag;
    @Mock private AmazonS3 amazonS3;
    @Mock private FileInputStream fileInputStream;
    @Mock private UploadPartResult uploadPartResult;
    @Mock private BufferedInputStream bufferedInputStream;
    @Mock private MultipartUploadOutputStream outputStream;

    private MultipartUpload multipartUpload;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        PowerMockito.mockStatic(FileUtils.class);
        PowerMockito.mockStatic(FileInputStream.class);
        PowerMockito.mockStatic(BufferedInputStream.class);

        this.multipartUpload = spy(new MultipartUpload(outputStream, 1, partFile, null));

        when(partFile.length()).thenReturn(10L);
        when(outputStream.getKey()).thenReturn("ceph_key");
        when(outputStream.getUploadId()).thenReturn("upload_id");
        when(outputStream.getBucketName()).thenReturn("ceph_bucket");
        when(outputStream.getAmazonS3()).thenReturn(amazonS3);
        PowerMockito.whenNew(FileInputStream.class).withAnyArguments().thenReturn(fileInputStream);
        PowerMockito.whenNew(BufferedInputStream.class).withAnyArguments().thenReturn(bufferedInputStream);
    }

    @Test
    public void testCallSuccess() throws Exception {
        PowerMockito.doNothing().when(FileUtils.class, "forceDelete", any());
        when(amazonS3.uploadPart(any())).thenReturn(uploadPartResult);
        when(uploadPartResult.getPartETag()).thenReturn(partETag);
        PartETag actual = multipartUpload.call();
        assertNotNull(actual);

        verify(partFile, times(1)).length();
        verify(outputStream, times(2)).getKey();
        verify(outputStream, times(1)).getUploadId();
        verify(outputStream, times(2)).getBucketName();
        verify(outputStream, times(1)).getAmazonS3();
        verify(amazonS3, times(1)).uploadPart(any());
        verify(uploadPartResult, times(1)).getPartETag();
        verifyNew(FileInputStream.class).withArguments(any());
        verifyNew(BufferedInputStream.class).withArguments(any());
    }

    @Test
    public void testCallFailure() throws Exception {
        boolean isException = false;
        when(amazonS3.uploadPart(any())).thenThrow(new AmazonClientException("Error"));
        when(partFile.delete()).thenReturn(true);

        try {
            multipartUpload.call();
        } catch (CephIngestionException e) {
            isException = true;
            assertTrue(e.getMessage().contains("Exception in Uploading part "));
        }
        assertTrue(isException);

        verify(partFile, times(1)).length();
        verify(outputStream, times(2)).getKey();
        verify(outputStream, times(1)).getUploadId();
        verify(outputStream, times(2)).getBucketName();
        verify(outputStream, times(1)).getAmazonS3();
        verify(amazonS3, times(1)).uploadPart(any());
        verify(partFile, times(1)).delete();
        verifyNew(FileInputStream.class).withArguments(any());
        verifyNew(BufferedInputStream.class).withArguments(any());
    }

}
