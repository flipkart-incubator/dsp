package com.flipkart.manager;

import com.flipkart.enums.TestExecutionStatus;
import com.flipkart.exception.JarException;
import com.flipkart.exception.TestScenarioExecutionException;
import com.flipkart.team.dsp.RequestStatusCheck;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.*;
import java.nio.file.Path;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class JarManager {
    private final RequestStatusCheck requestStatusCheck;

    public void downloadJarFromUriToLocalPath(Path localJarPath, String jarUri) throws IOException {
        log.info("Loading Sandbox CLI Jar ...");
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(jarUri);
            try (CloseableHttpResponse response = client.execute(httpGet)) {
                HttpEntity responseEntity = response.getEntity();
                try (InputStream urlContent = responseEntity.getContent();
                     BufferedOutputStream outputFile = new BufferedOutputStream(new FileOutputStream(localJarPath.toFile()))
                ) {
                    int inByte;
                    while ((inByte = urlContent.read()) != -1) {
                        outputFile.write(inByte);
                    }

                }
            }
         log.info("Finished Downloading Sandbox CLI Jar");
        }
    }

    public Object executeJar(String[] processBuilderCommand) throws JarException, TestScenarioExecutionException, InterruptedException  {
        try {
            BufferedReader in = executeProcessBuilder(processBuilderCommand);
            String input;
            Long requestId = 0L;
            while ((input = in.readLine()) != null) {
                log.info(input);
                if (input.startsWith("Run Id: ")) {
                    String[] val = input.split(":");
                    if (val.length == 2) {
                        requestId = Long.valueOf(val[1].trim());
                    }
                }

                if (requestId != 0L) {
                    TestExecutionStatus requestStatus = requestStatusCheck.getRequestStatus(String.valueOf(requestId));
                    if (requestStatus.equals(TestExecutionStatus.FAILED)) {
                        throw new TestScenarioExecutionException("Request Failed while Running flow from Sandbox");
                    } else if (requestStatus.equals(TestExecutionStatus.PASSED))
                        return requestId;
                    else
                        Thread.sleep(5000);
                }
            }
            return requestId;
        } catch (IOException  e) {
            throw new JarException("Issue in Executing jar " + e.getMessage());
        }
    }

    public Object executeJar(String[] processBuilderCommand,  String errorMessage) throws JarException {
        try {
            BufferedReader in = executeProcessBuilder(processBuilderCommand);
            String input;
            StringBuffer stringBuffer = new StringBuffer();
            while ((input = in.readLine()) != null) {
                log.info(input);
                stringBuffer.append(input);
            }
            return stringBuffer;
        } catch (IOException e) {
            throw new JarException(errorMessage + ": " + e.getMessage());
        }
    }

    private BufferedReader executeProcessBuilder(String[] processBuilderCommand) throws IOException {
        ProcessBuilder pb = new ProcessBuilder();
        pb.command(processBuilderCommand);
        Process p = pb.start();
        return new BufferedReader(new InputStreamReader(p.getInputStream()));
    }
}
