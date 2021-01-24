package com.flipkart.dsp.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExecutionEnvironmentDetails {
    Map<String, String> library;
    Map<String, NativeLibraryDetails> nativeLibrary;
    String os;
    String osVersion;
    String imageLanguageVersion;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NativeLibraryDetails {
        String version;
        String architecture;
    }
}
