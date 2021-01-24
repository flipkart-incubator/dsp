package com.flipkart.dsp.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class GithubConfig {
    private String apiUrl;
    private String login;
    private String token;
    private String commaSeparatedAllowedFileTypes;
    private String oAuthToken; // Getting used in regression
    private String scriptCacheBasePath = "/tmp/dsp-script-cache";
    public Set<String> getAllowedFileExtensions()  {
        return new HashSet<>(Arrays.asList(commaSeparatedAllowedFileTypes.split(",")));
    }
}
