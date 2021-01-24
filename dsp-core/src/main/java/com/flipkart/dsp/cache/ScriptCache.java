package com.flipkart.dsp.cache;

import com.flipkart.dsp.config.GithubConfig;
import com.google.inject.Inject;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

import static com.flipkart.dsp.utils.Constants.ZIP_EXTENSION;

@Slf4j
@Getter
public class ScriptCache {

    private String localBaseDir;

    @Inject
    public ScriptCache(GithubConfig githubConfig) {
        this.localBaseDir = githubConfig.getScriptCacheBasePath();
    }

    public String getLocalDirectoryLocation(ScriptKey scriptKey) {
        return getLocalBaseDir() + File.separator + scriptKey.getKey() + UUID.randomUUID().toString();
    }

    public boolean isAvailableInCache(ScriptKey scriptKey) {
        return Files.exists(Paths.get(getScriptFolderZipLocation(scriptKey)));
    }

    public String getScriptFolderZipLocation(ScriptKey scriptKey) {
        return getLocalBaseDir() + File.separator + scriptKey.getKey() + ZIP_EXTENSION;
    }
}
