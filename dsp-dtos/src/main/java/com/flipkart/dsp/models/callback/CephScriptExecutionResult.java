package com.flipkart.dsp.models.callback;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.net.URL;
import java.util.List;

@Data
@AllArgsConstructor
public class CephScriptExecutionResult implements ScriptExecutionResult {
    private String bucket;
    private String path;
    private List<URL> urls;
}
