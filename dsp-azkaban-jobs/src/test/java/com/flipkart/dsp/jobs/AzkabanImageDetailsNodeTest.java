package com.flipkart.dsp.jobs;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.dsp.actors.ExecutionEnvironmentActor;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.exceptions.DockerRegistryClientException;
import com.flipkart.dsp.exceptions.AzkabanException;
import com.flipkart.dsp.helper.ImageDetailsHelper;
import com.flipkart.dsp.mesos.ImageDetailsMesosExecutionDriver;
import com.flipkart.dsp.models.*;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.utils.JsonUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static io.dropwizard.testing.FixtureHelpers.fixture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.*;

/**
 *
 */
public class AzkabanImageDetailsNodeTest {
    @Mock
    private DSPServiceClient dspServiceClient;
    @Mock
    private ImageDetailsHelper imageDetailsHelper;
    @Mock
    private ExecutionEnvironmentActor executionEnvironmentActor;
    @Mock
    private ExecutionEnvironmentSummary executionEnvironmentSummary;
    @Mock
    private ImageDetailsMesosExecutionDriver imageDetailsMesosExecutionDriver;

    private String latestImageDigest = "latestImageDigest";
    private ExecutionEnvironmentSummary executionEnvironment;
    private ExecutionEnvironmentSnapshot executionEnvironmentSnapshot;

    private AzkabanImageDetailsNode azkabanImageDetailsNode;
    private List<ExecutionEnvironmentSummary> executionEnvironmentSummaries = new ArrayList<>();
    private List<ExecutionEnvironmentSnapshot> executionEnvironmentSnapshots = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        azkabanImageDetailsNode = spy(new AzkabanImageDetailsNode(dspServiceClient, imageDetailsHelper, executionEnvironmentActor, imageDetailsMesosExecutionDriver));
        executionEnvironmentSummaries.add(executionEnvironmentSummary);

        String testLanguage = "PYTHON3";
        Map<String, String> librarySet = JsonUtils.DEFAULT.mapper.readValue(fixture("fixtures/library_set.json"), new TypeReference<Map<String, String>>() {
        });
        executionEnvironmentSnapshot = new ExecutionEnvironmentSnapshot();
        executionEnvironmentSnapshot.setLibrarySet(JsonUtils.DEFAULT.mapper.writeValueAsString(librarySet));
        executionEnvironmentSnapshot.setVersion(1);
        executionEnvironmentSnapshot.setLatestImageDigest("imageDigest");
        executionEnvironmentSnapshots.add(executionEnvironmentSnapshot);

        executionEnvironment = new ExecutionEnvironmentSummary();
        ExecutionEnvironmentSummary.Specification specification = ExecutionEnvironmentSummary.Specification.builder()
                .language(ImageLanguageEnum.valueOf(testLanguage)).build();
        executionEnvironment.setImagePath("0.0.0.0/jessie-py:3.4.2");
        executionEnvironmentSummaries.add(executionEnvironment);
    }

    @Test
    public void testGetName() {
        assertEquals(azkabanImageDetailsNode.getName(), Constants.IMAGE_DETAILS_NODE);
    }

    @Test
    public void testExecuteSuccessCase1() throws Exception {
        when(executionEnvironmentActor.getExecutionEnvironmentsSummary()).thenReturn(executionEnvironmentSummaries);
        when(imageDetailsHelper.getLatestImageDigest(any())).thenReturn(latestImageDigest);
        when(imageDetailsHelper.getLatestImageDigestInDb(executionEnvironment)).thenReturn(latestImageDigest);
        when(imageDetailsHelper.isImageUpdated(latestImageDigest, latestImageDigest)).thenReturn(true);
        doNothing().when(imageDetailsMesosExecutionDriver).execute(any());

        azkabanImageDetailsNode.execute(null);
        verify(executionEnvironmentActor, times(1)).getExecutionEnvironmentsSummary();
        verify(imageDetailsHelper, times(2)).getLatestImageDigest(any());
        verify(imageDetailsHelper, times(1)).getLatestImageDigestInDb(executionEnvironment);
        verify(imageDetailsHelper, times(1)).isImageUpdated(latestImageDigest, latestImageDigest);
        verify(imageDetailsMesosExecutionDriver, times(1)).execute(any());
    }

    @Test
    public void testExecuteSuccessCase2() throws Exception {
        String latestImageDigest1 = "imageDigest1";
        when(executionEnvironmentActor.getExecutionEnvironmentsSummary()).thenReturn(executionEnvironmentSummaries);
        when(imageDetailsHelper.getLatestImageDigest(any())).thenReturn(latestImageDigest);
        when(imageDetailsHelper.getLatestImageDigestInDb(any())).thenReturn(latestImageDigest1);
        when(imageDetailsHelper.isImageUpdated(latestImageDigest, latestImageDigest1)).thenReturn(false);
        doNothing().when(imageDetailsMesosExecutionDriver).execute(any());

        azkabanImageDetailsNode.execute(null);
        verify(executionEnvironmentActor, times(1)).getExecutionEnvironmentsSummary();
        verify(imageDetailsHelper, times(2)).getLatestImageDigest(any());
        verify(imageDetailsHelper, times(2)).getLatestImageDigestInDb(any());
        verify(imageDetailsHelper, times(2)).isImageUpdated(latestImageDigest, latestImageDigest1);
        verify(imageDetailsMesosExecutionDriver, times(1)).execute(any());
    }

    @Test
    public void testExecuteFailureCase1() throws Exception {
        boolean isException = false;
        when(executionEnvironmentActor.getExecutionEnvironmentsSummary()).thenReturn(executionEnvironmentSummaries);
        when(imageDetailsHelper.getLatestImageDigest(any())).thenThrow(new DockerRegistryClientException("Exception"));

        try {
            azkabanImageDetailsNode.execute(null);
        } catch (AzkabanException e) {
            isException = true;
            assertEquals(e.getMessage(), "Azkaban node failed because of following reason:");
        }
        assertTrue(isException);
        verify(executionEnvironmentActor, times(1)).getExecutionEnvironmentsSummary();
        verify(imageDetailsHelper, times(1)).getLatestImageDigest(any());
    }
}
