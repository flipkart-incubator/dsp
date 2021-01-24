package com.flipkart.dsp.utils;

import com.flipkart.dsp.config.DSPServiceConfig;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class PathHelper {
    private final static String WEB_HDFS_FORMAT = "http://%s:%s/explorer.html#%s";

    private final DSPServiceConfig.HDFSConfig hdfsConfig;

    public Map<String, String> getDFHDFSCompletePath(Map<String/** dataframeName */, String/** WebHdfs location */> inputDetails) {
        return inputDetails.entrySet().stream().collect(Collectors.toMap(x -> x.getKey(), y -> String.format(WEB_HDFS_FORMAT,
                hdfsConfig.getActiveNameNode(), hdfsConfig.getPort(), y.getValue())));
    }
}
