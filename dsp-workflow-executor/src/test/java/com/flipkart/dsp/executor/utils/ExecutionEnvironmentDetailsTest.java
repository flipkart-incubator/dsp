package com.flipkart.dsp.executor.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.models.ExecutionEnvironmentDetails;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ExecutionEnvironmentDetailsTest {

    @Test
    public void bashResponseSerializationTest() throws IOException, URISyntaxException {
        ExecutionEnvironmentDetails executionEnvironmentDetails = getWorkflowSandboxDetailsObject();
        Assert.assertNotNull(executionEnvironmentDetails);
    }

    private ExecutionEnvironmentDetails getWorkflowSandboxDetailsObject() throws URISyntaxException, IOException {
        Path expectedFilePath = getPath("/bash_response_test.json");
        String expectedValue =  new String(Files.readAllBytes(expectedFilePath), StandardCharsets.UTF_8);
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(expectedValue, ExecutionEnvironmentDetails.class);
    }

    private Path getPath(String filePath) throws URISyntaxException {
        URL url = getClass().getResource(filePath);
        return Paths.get(url.toURI().getPath());
    }
}
