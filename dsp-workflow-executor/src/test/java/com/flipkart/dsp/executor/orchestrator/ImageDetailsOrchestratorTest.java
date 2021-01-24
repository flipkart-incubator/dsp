package com.flipkart.dsp.executor.orchestrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.DockerRegistryClient;
import com.flipkart.dsp.entities.misc.ImageDetailPayload;
import com.flipkart.dsp.entities.script.LocalScript;
import com.flipkart.dsp.executor.extractor.ExtractImageSpecificLibrary;
import com.flipkart.dsp.executor.runner.ScriptRunner;
import com.flipkart.dsp.models.ExecutionEnvironmentSnapshot;
import com.flipkart.dsp.models.ExecutionEnvironmentSummary;
import com.flipkart.dsp.models.ScriptVariable;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 */
public class ImageDetailsOrchestratorTest {
    private LocalScript script;
    private ImageDetailPayload imageDetailPayload;
    private ScriptRunner scriptRunner = mock(ScriptRunner.class);
    private ImageDetailsOrchestrator imageDetailsOrchestrator;
    private Set<ScriptVariable> outputVariables = new HashSet<>();
    private String latestImageDigest = "testingLatestImageDigest";
    private DSPServiceClient dspServiceClient = mock(DSPServiceClient.class);
    private DockerRegistryClient dockerRegistryClient = mock(DockerRegistryClient.class);
    private List<ExecutionEnvironmentSnapshot> executionEnvironmentSnapshotList = new ArrayList<>();
    private ObjectMapper objectMapper = new ObjectMapper();
    private ExtractImageSpecificLibrary extractImageSpecificLibrary = new ExtractImageSpecificLibrary(new ObjectMapper());

    @Before
    public void setup() throws IOException, URISyntaxException {
        this.imageDetailsOrchestrator = spy(new ImageDetailsOrchestrator(objectMapper, scriptRunner, dspServiceClient, dockerRegistryClient, extractImageSpecificLibrary));

        script = new LocalScript();
        ScriptVariable outputVariable = ScriptVariable.builder().value(getBashExecutionOutput()).build();
        outputVariables.add(outputVariable);
        script.setOutputVariables(outputVariables);

        long executionEnvironmentId = 1;
        ExecutionEnvironmentSummary executionEnvironment = ExecutionEnvironmentSummary.builder().id(executionEnvironmentId)
            .imagePath("0.0.0.0/jessie-r:3.2.5").build();
        imageDetailPayload = ImageDetailPayload.builder().executionEnvironmentSnapshots(executionEnvironmentSnapshotList)
                .executionEnvironmentSummary(executionEnvironment).build();
    }

    private Object getBashExecutionOutput() throws IOException, URISyntaxException {
        Path expectedFilePath = getPath("/bash_response_test.json");
        Object object = new String(Files.readAllBytes(expectedFilePath), StandardCharsets.UTF_8);
        return object;
    }

    private Path getPath(String filePath) throws URISyntaxException {
        URL url = getClass().getResource(filePath);
        return Paths.get(url.toURI().getPath());
    }

    //Run(LocalScript, ImageDetailPayload)
    @Test
    public void testRunSuccessCase2() throws Exception {

        when(scriptRunner.run(script)).thenReturn(outputVariables);
        doNothing().when(dspServiceClient).saveExecutionEnvironmentSnapshotRequest(any());
        when(dockerRegistryClient.getLatestImageDigest(anyString())).thenReturn(latestImageDigest);

        imageDetailsOrchestrator.run(script, imageDetailPayload);
        verify(scriptRunner, times(1)).run(script);
        verify(dspServiceClient, times(1)).saveExecutionEnvironmentSnapshotRequest(any());
        verify(dockerRegistryClient, timeout(1)).getLatestImageDigest(anyString());
    }

    //Run(LocalScript, ImageDetailPayload)
    @Test
    public void testRunSuccessCase3() throws Exception {

        ExecutionEnvironmentSnapshot executionEnvironmentSnapshot = ExecutionEnvironmentSnapshot.builder().version(1).build();
        executionEnvironmentSnapshotList.add(executionEnvironmentSnapshot);
        when(scriptRunner.run(script)).thenReturn(outputVariables);
        doNothing().when(dspServiceClient).saveExecutionEnvironmentSnapshotRequest(any());
        when(dockerRegistryClient.getLatestImageDigest(anyString())).thenReturn(latestImageDigest);

        imageDetailsOrchestrator.run(script, imageDetailPayload);
        verify(scriptRunner, times(1)).run(script);
        verify(dspServiceClient, times(1)).saveExecutionEnvironmentSnapshotRequest(any());
        verify(dockerRegistryClient, timeout(1)).getLatestImageDigest(anyString());
    }
}
