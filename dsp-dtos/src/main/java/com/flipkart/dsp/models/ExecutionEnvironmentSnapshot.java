package com.flipkart.dsp.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExecutionEnvironmentSnapshot implements Serializable {
    private String os;
    private long version;
    private String osVersion;
    private String librarySet;
    private String nativeLibrarySet;
    private String latestImageDigest;
    private long executionEnvironmentId;
    private String imageLanguageVersion;
}
