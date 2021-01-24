package com.flipkart.dsp.executor.utils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class ExecutorLogManager {
    private String LOG_URL_TEMPLATE = "http://%s:5051/files/read?path=%%2Fvar%%2Flib%%2Fmesos%%2Fslaves%%2F%s%%2Fframeworks%%2F%s%%2Fexecutors%%2F%s%%2Fruns%%2F%s%%2F";
    private String ENCODING = "UTF-8";

    public String getMesosLogURL(String hostIp, String slaveId, String frameworkId, String executorId) throws UnsupportedEncodingException {
        return String.format(LOG_URL_TEMPLATE, hostIp, URLEncoder.encode(slaveId, ENCODING), URLEncoder.encode(frameworkId, ENCODING), URLEncoder.encode(executorId, ENCODING),URLEncoder.encode("latest", ENCODING));
    }

    public String getMesosLogURLByContainerId(String hostIp, String slaveId, String frameworkId, String executorId, String containerId) throws UnsupportedEncodingException {
        return String.format(LOG_URL_TEMPLATE, hostIp, URLEncoder.encode(slaveId, ENCODING), URLEncoder.encode(frameworkId, ENCODING), URLEncoder.encode(executorId, ENCODING), URLEncoder.encode(containerId, ENCODING));
    }
}
