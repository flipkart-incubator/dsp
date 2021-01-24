package com.flipkart.dsp.executor.application;

import com.flipkart.dsp.client.exceptions.DockerRegistryClientException;
import com.flipkart.dsp.config.MiscConfig;
import com.flipkart.dsp.engine.exception.ScriptExecutionEngineException;
import com.flipkart.dsp.entities.misc.ImageDetailPayload;
import com.flipkart.dsp.entities.script.LocalScript;
import com.flipkart.dsp.executor.exception.ApplicationException;
import com.flipkart.dsp.executor.exception.ExtractImageSpecificLibraryException;
import com.flipkart.dsp.executor.orchestrator.ImageDetailsOrchestrator;
import com.flipkart.dsp.models.ImageLanguageEnum;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.utils.JsonUtils;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;

/**
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ImageDetailsMesosApplication extends AbstractApplication {

    private final MiscConfig miscConfig;
    private final ImageDetailsOrchestrator imageDetailsOrchestrator;
    private static final String EXECUTOR_NAME = "ImageDetailsMesosApplication";

    @Override
    public String getName() {
        return EXECUTOR_NAME;
    }

    @Override
    public void execute(String[] args) throws ApplicationException {
        String payload = args[0];
        ImageDetailPayload imageDetailPayload = deserializePayload(payload);
        log.info("imageDetail payload " + payload);

        try {
            LocalScript script = new LocalScript();
            script.setExecutionEnvironment("BASH");
            script.setImageLanguageEnum(ImageLanguageEnum.BASH);
            script.setLocation(String.format(Constants.IMAGE_DETAILS_SCRIPT_PATH, miscConfig.getExecutorJarVersion(), miscConfig.getEnvironment())
                    + Constants.slash + Constants.IMAGE_DETAILS_SCRIPT_NAME);
            ScriptVariable scriptVariable = ScriptVariable.builder().value(imageDetailPayload.getExecutionEnvironmentSummary()
                    .getSpecification().getLanguage().name()).build();
            script.setInputVariables(new HashSet<>(Collections.singletonList(scriptVariable)));
            imageDetailsOrchestrator.run(script, imageDetailPayload);
        } catch (ScriptExecutionEngineException | DockerRegistryClientException | IOException | ExtractImageSpecificLibraryException e) {
            log.error("Failed to run application {} because of following reason: ", getName(), e);
            throw new ApplicationException(getName(), e);
        }
    }

    private ImageDetailPayload deserializePayload(String payload) throws ApplicationException {
        try {
            return JsonUtils.DEFAULT.mapper.readValue(payload, ImageDetailPayload.class);
        } catch (IOException e) {
            log.error("Error while deserialize payload. Payload : {}", payload, e);
            throw new ApplicationException(getName(), e);
        }
    }
}
