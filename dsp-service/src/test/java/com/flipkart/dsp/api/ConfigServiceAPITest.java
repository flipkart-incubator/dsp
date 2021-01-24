//package com.flipkart.dsp.api;
//
//import com.flipkart.dsp.client.ConfigServiceClient;
//import com.flipkart.dsp.utils.Constants;
//import com.flipkart.kloud.config.DynamicBucket;
//import com.flipkart.kloud.config.error.ConfigServiceException;
//import org.junit.Before;
//import org.junit.Test;
//import org.mockito.Mock;
//import org.mockito.MockitoAnnotations;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//import static org.mockito.Mockito.*;
//
///**
// * +
// */
//public class ConfigServiceAPITest {
//    @Mock private DynamicBucket dynamicBucket;
//    @Mock private ConfigServiceClient configServiceClient;
//
//    private String env = "dev";
//    private String bucket  = env + "-azk-version";
//    private String azkJarVersion = "11.0";
//    private ConfigServiceAPI configServiceAPI;
//
//    @Before
//    public void setUp() {
//        MockitoAnnotations.initMocks(this);
//        this.configServiceAPI = spy(new ConfigServiceAPI(configServiceClient));
//    }
//
//    @Test
//    public void testGetAzkabanJarVersionSuccess() throws Exception {
//        when(configServiceClient.getConfigBucket(bucket)).thenReturn(dynamicBucket);
//        when(dynamicBucket.getString(Constants.AZKABAN_JAR_VERSION)).thenReturn(azkJarVersion);
//        when(configServiceClient.makeDynamicBucket(bucket, env)).thenReturn(dynamicBucket);
//
//        String expected = configServiceAPI.getAzkabanJarVersion(env);
//        assertEquals(expected, azkJarVersion);
////        verify(configServiceClient, times(1)).makeDynamicBucket(bucket, env);
//        verify(configServiceClient, times(1)).getConfigBucket(bucket);
//        verify(dynamicBucket, times(1)).getString(Constants.AZKABAN_JAR_VERSION);
//    }
//
//    @Test
//    public void testGetAzkabanJarVersionFailure() throws Exception {
//        boolean isException = false;
//        when(configServiceClient.getConfigBucket(bucket)).thenThrow(new ConfigServiceException("Exception", ConfigServiceException.TYPE.NOT_FOUND));
//
//        try {
//            configServiceAPI.getAzkabanJarVersion(env);
//        } catch (ConfigServiceException e) {
//            isException = true;
//            assertEquals(e.getMessage(), "Unable to find Azkaban Jar Version in Config " + bucket);
//        }
//
//        assertTrue(isException);
////        verify(configServiceClient, times(1)).makeDynamicBucket(bucket, env);
//        verify(configServiceClient, times(1)).getConfigBucket(bucket);
//    }
//}
