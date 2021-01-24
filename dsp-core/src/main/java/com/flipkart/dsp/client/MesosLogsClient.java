package com.flipkart.dsp.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.entities.misc.MesosLogs;
import com.flipkart.dsp.exceptions.MesosLogsClientException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Singleton
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class MesosLogsClient {

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public String getLogs(String logLocation, Integer logOffset, String logType) throws MesosLogsClientException {
        StringBuilder logString = new StringBuilder();
        while (true) {
            String url = logLocation + logType + "&offset=" + logOffset;
            MesosLogs mesosLogs = getLogs(url);
            logOffset = logOffset + mesosLogs.getData().length();
            logString.append(filterLogs(mesosLogs.getData()));
            if (mesosLogs.getData().length() == 0) break;
        }
        return logString.toString();
    }

    private MesosLogs getLogs(String url) throws MesosLogsClientException {
        MesosLogs mesosLogs = null;
        try {
            HttpResponse response = httpClient.execute(new HttpGet(url));
            int responceCode = response.getStatusLine().getStatusCode();
            if (responceCode != 200) {
                log.error("Failed to retrieve Mesos logs for {} with error code {}", url, responceCode);
                throw new MesosLogsClientException("Failed to retrieve Mesos logs for" + url + " with error code " + responceCode);
            }
            mesosLogs = objectMapper.readValue(EntityUtils.toString(response.getEntity()), MesosLogs.class);
        } catch (IOException e) {
            log.error("Failed to retrieve Mesos logs for {}", url, e);
            throw new MesosLogsClientException("Failed to retrieve Mesos logs", e);
        }
        return mesosLogs;
    }

    private String filterLogs(String logs) {
        StringBuilder logString = new StringBuilder();
        String[] logData = logs.split("\n");
        for(String line : logData) {
            if(!line.matches("^([0-9]+ com\\..*$)|(10\\..*$)")) {
                logString.append(line).append("\n");
            }
        }
        return logString.toString();
    }
}
