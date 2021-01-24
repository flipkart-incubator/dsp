package com.flipkart.dsp.executor.utils;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.cache.ScriptKey;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.exceptions.DSPClientProcessingException;
import com.flipkart.dsp.client.script.DownloadScriptFolderRequest;
import com.flipkart.dsp.config.DSPClientConfig;
import com.flipkart.dsp.config.DSPServiceConfig;
import com.flipkart.dsp.entities.script.LocalScript;
import com.flipkart.dsp.entities.script.Script;
import com.flipkart.dsp.entities.script.ScriptMeta;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.utils.ZipUtils;
import com.google.inject.Inject;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

@RequiredArgsConstructor(onConstructor = @__(@Inject))
@Slf4j
public class ScriptHelper {
    private final DSPServiceClient dspServiceClient;
    private final DSPServiceConfig.ScriptExecutionConfig scriptExecutionConfig;

    public LocalScript importScript(@NonNull Script script) throws DSPClientProcessingException {
        return downloadScriptFolderByScriptId(script.getId());
    }

    @Metered
    @Timed
    public LocalScript downloadScriptFolderByScriptId(long scriptId) throws DSPClientProcessingException {
        try {
            ScriptMeta script = dspServiceClient.getScriptMetaById(scriptId);
            ScriptKey scriptKey = new ScriptKey(script.getGitRepo(), script.getGitFolder(), script.getGitCommitId());

            String scriptBaseDir = scriptExecutionConfig.getWorkingDir();
            String location = scriptBaseDir + File.separator + scriptKey.getKey() + "__" + UUID.randomUUID().toString();
            Files.createDirectories(Paths.get(location));

            String zipFileLocation = scriptBaseDir + UUID.randomUUID().toString() + Constants.ZIP_EXTENSION;
            try (InputStream inputStream = new DownloadScriptFolderRequest(scriptId, dspServiceClient).executeSync();
                 OutputStream outputStream = new FileOutputStream(zipFileLocation)) {
                IOUtils.copy(inputStream, outputStream);
            }

            ZipUtils.unzipFileIntoDirectory(zipFileLocation, location, true);
            String scriptFolderLocation = getLocalScriptFolderLocation(location);
            return new LocalScript(scriptId, script.getGitFilePath(), scriptFolderLocation, script.getExecEnv(),
                    script.getInputVariables(), script.getOutputVariables(), script.getImageLanguageEnum());
        } catch (IOException ioe) {
            log.error("Exception received during download scriptFolder : ", ioe);
            throw new DSPClientProcessingException("Exception received during download scriptFolder : ", ioe);
        }
    }

    private String getLocalScriptFolderLocation(String location) throws DSPClientProcessingException {
        File[] dirLocation = new File(location).listFiles(File::isDirectory);
        if (dirLocation.length != 1) {
            throw new DSPClientProcessingException("Fatal : zip file contain " + dirLocation.length + " directories !!!");
        } else {
            return dirLocation[0].getAbsolutePath();
        }
    }
}
