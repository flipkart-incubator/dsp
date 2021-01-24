package com.flipkart.dsp.api;


import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.dsp.actors.ScriptActor;
import com.flipkart.dsp.cache.ScriptCache;
import com.flipkart.dsp.cache.ScriptKey;
import com.flipkart.dsp.client.GithubClient;
import com.flipkart.dsp.dao.ScriptDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.ExecutionEnvironmentEntity;
import com.flipkart.dsp.db.entities.ScriptEntity;
import com.flipkart.dsp.entities.script.Script;
import com.flipkart.dsp.entities.script.ScriptMeta;
import com.flipkart.dsp.exception.DSPSvcException;
import com.flipkart.dsp.exceptions.ValidationException;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.workflow.CreateWorkflowRequest;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.utils.JsonUtils;
import com.flipkart.dsp.utils.ZipUtils;
import com.flipkart.dsp.validation.ScriptValidator;
import com.flipkart.dsp.validation.Validator;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ScriptAPI {
    private final ScriptDAO scriptDAO;
    private final Validator validator;
    private final ScriptActor scriptActor;
    private final ScriptCache scriptCache;
    private final GithubClient githubClient;
    private final ScriptValidator scriptValidator;
    private final TransactionLender transactionLender;
    private ConcurrentHashMap<String, Long> currentDownloadingScript = new ConcurrentHashMap<>();

    public String getScriptDirectoryZipFile(Long scriptId) throws DSPSvcException {
        Script script = validator.verifyScriptId(scriptId);
        try {
            ScriptKey scriptKey = new ScriptKey(script.getGitRepo(), script.getGitFolder(), script.getGitCommitId());
            String zipFileLocation = scriptCache.getScriptFolderZipLocation(scriptKey);
            if (!scriptCache.isAvailableInCache(scriptKey)) {
                if (shouldDownload(scriptKey)) downloadScript(scriptKey, zipFileLocation);
                else waitForScriptDownload(scriptKey, zipFileLocation);
            }
            return zipFileLocation;
        } catch (IOException ioe) {
            log.error("Exception received in fetching script folder content from github for script {} ", script);
            throw new DSPSvcException("Exception received in script retrieval from github ", ioe);
        }
    }

    private boolean shouldDownload(ScriptKey scriptKey) {
        return ((currentDownloadingScript.containsKey(scriptCache.getScriptFolderZipLocation(scriptKey))
                && (new Date().getTime() - currentDownloadingScript.get((scriptCache.getScriptFolderZipLocation(scriptKey))) > 120000))
                || (!currentDownloadingScript.containsKey(scriptCache.getScriptFolderZipLocation(scriptKey))));
    }

    private void downloadScript(ScriptKey scriptKey, String zipFileLocation) throws IOException {
        currentDownloadingScript.put(scriptCache.getScriptFolderZipLocation(scriptKey), new Date().getTime());
        try {
            String localDirectory = scriptCache.getLocalDirectoryLocation(scriptKey);
            githubClient.fetchDirectoryContent(scriptKey.getGitRepo(), scriptKey.getGitCommitId(), scriptKey.getGitFolder(), localDirectory, true);
            log.debug("Completed fetch from github to local directory : {} ", localDirectory);
            ZipUtils.zipDirectory(localDirectory, zipFileLocation, true);
            log.debug("Zip completed and location of zip file : {} ", zipFileLocation);
        } finally {
            currentDownloadingScript.remove(scriptCache.getScriptFolderZipLocation(scriptKey));
        }
    }

    private void waitForScriptDownload(ScriptKey scriptKey, String zipFileLocation) throws IOException {
        while (scriptCache.isAvailableInCache(scriptKey)) {
            try {
                if (shouldDownload(scriptKey)) {
                    log.info("Previous Script download call is taking too much time to download. Downloading script again " + scriptKey.getGitCommitId());
                    downloadScript(scriptKey, zipFileLocation);
                } else {
                    log.info("Waiting for the script " + scriptKey.getGitCommitId() + " to be downloaded");
                    Thread.sleep(2000);
                }
            } catch (InterruptedException e) {
                // Nothing to worry we can ignore this
            }
        }
    }

    Script prepareScript(CreateWorkflowRequest.Script scriptRequest) throws ValidationException {
        Script existingScript = scriptActor.getScriptByGitDetails(scriptRequest.getGitRepo(), scriptRequest.getGitFolderPath(),
                scriptRequest.getFilePath(), scriptRequest.getGitCommitId(), false);
        Double version = Objects.isNull(existingScript) ? 1.0 : existingScript.getVersion() + 1;
        Script script = Script.builder().gitRepo(scriptRequest.getGitRepo()).gitFolder(scriptRequest.getGitFolderPath())
                .filePath(scriptRequest.getFilePath()).gitCommitId(scriptRequest.getGitCommitId()).isDraft(true)
                .executionEnvironment(scriptRequest.getExecutionEnv())
                .inputVariables(scriptRequest.getInputs())
                .outputVariables(scriptRequest.getOutputs()).version(version).build();
        scriptValidator.validateScriptInputs(scriptRequest);
        return script;
    }

    public ScriptMeta getScriptMeta(Long id) {
        final AtomicReference<ScriptMeta> scriptAtomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                ScriptEntity scriptEntity = scriptDAO.get(id);
                scriptAtomicReference.set(convertScriptToScriptMeta(scriptEntity));
            }
        });
        if (scriptAtomicReference.get()!= null) {
            return scriptAtomicReference.get();
        } else {
            throw new RuntimeException("No scriptEntity available with id : " + id);
        }
    }

    private ScriptMeta convertScriptToScriptMeta(ScriptEntity script) {
        ExecutionEnvironmentEntity executionEnvironmentEntity = script.getExecutionEnvironmentEntity();
        String imagePath = String.format(Constants.IMAGE_PATH_FORMAT, executionEnvironmentEntity.getDockerHub(),
                executionEnvironmentEntity.getImageIdentifier(), executionEnvironmentEntity.getImageVersion());
        Set<ScriptVariable> inputScriptVariableSet = JsonUtils.DEFAULT.fromJson(script.getInputVariables(), new TypeReference<Set<ScriptVariable>>() {});
        Set<ScriptVariable> outputScriptVariableSet = JsonUtils.DEFAULT.fromJson(script.getOutputVariables(), new TypeReference<Set<ScriptVariable>>() {});
        return ScriptMeta.builder().id(script.getId()).imagePath(imagePath).execEnv(executionEnvironmentEntity.getExecutionEnvironment())
                .gitFilePath(script.getGitFilePath()).gitCommitId(script.getGitCommitId()).gitFolder(script.getGitFolder())
                .gitRepo(script.getGitRepo()).startUpScriptPath(executionEnvironmentEntity.getStartUpScriptPath())
                .inputVariables(inputScriptVariableSet).outputVariables(outputScriptVariableSet)
                .imageLanguageEnum(script.getExecutionEnvironmentEntity().getImageLanguage()).build();

    }

}
