package com.flipkart.dsp.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;

/**
 * This is a library class which make all http calls.
 */

@RequiredArgsConstructor(onConstructor = @__(@Inject))
@Slf4j
public class HttpRequestClient {
    private static final String UTF_8 = "UTF-8";
    private final ObjectMapper objectMapper;

    /*This method is used to do get call through apache http client*/
    public <V extends Object> V getRequest(HttpURLConnection request, TypeReference typeReference) throws IOException {
        request.connect();
        String responseStr = IOUtils.toString((InputStream) request.getContent(), UTF_8);
        if (request.getResponseCode() != 200) {
            throw new IOException("Request failed with following error code: " + request.getResponseCode()
                    + " with error message: " + request.getResponseMessage());
        }
        Object object = objectMapper.readValue(responseStr, typeReference);
        request.disconnect();
        return (V) object;
    }

    public String getRequest(HttpURLConnection request) throws IOException {
        request.connect();
        return IOUtils.toString(request.getInputStream(), UTF_8);
    }


    public <V extends Object> V postRequest(HttpURLConnection request, Object payload, TypeReference typeReference) throws IOException {
        postRequest(request, payload);
        String responseStr = IOUtils.toString((InputStream) request.getContent(), UTF_8);
        return objectMapper.readValue(responseStr, typeReference);
    }

    public void postRequest(HttpURLConnection request, Object payload) throws IOException {
        String json = objectMapper.writeValueAsString(payload);
        log.info("Making Http POST Call with following payload: \n{}",json);
        OutputStream os = request.getOutputStream();
        os.write(json.getBytes(UTF_8));
        os.close();
        if (!(request.getResponseCode() == 200 || request.getResponseCode() == 202)) {
            throw new IOException("Request failed with following error code: " + request.getResponseCode()
                    + " with error message: " + request.getResponseMessage());
        }
    }

}
