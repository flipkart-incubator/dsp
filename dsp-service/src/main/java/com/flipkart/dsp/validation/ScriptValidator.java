package com.flipkart.dsp.validation;

import com.flipkart.dsp.actors.ExternalCredentialsActor;
import com.flipkart.dsp.client.GithubClient;
import com.flipkart.dsp.exceptions.DSPCoreException;
import com.flipkart.dsp.exceptions.ValidationException;
import com.flipkart.dsp.models.ExternalCredentials;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.outputVariable.CephOutputLocation;
import com.flipkart.dsp.models.outputVariable.OutputLocation;
import com.flipkart.dsp.models.workflow.CreateWorkflowRequest;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * +
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ScriptValidator {
    private final GithubClient githubClient;
    private final ExternalCredentialsActor externalCredentialsActor;

    public void validateScriptInputs(CreateWorkflowRequest.Script scriptRequest) throws ValidationException {
        try {
            if(!githubClient.isValidFileInGithubForGivenCommitId(scriptRequest.getGitRepo(), scriptRequest.getGitFolderPath(),
                    scriptRequest.getFilePath(), scriptRequest.getGitCommitId())){
                throw new IllegalArgumentException("Not a valid commitId or gitFilePath or gitFolder for given file  : " + scriptRequest.getFilePath());
            }
        } catch (IOException ioe) {
            log.error("Exception received in validation of file existence in github for script : " + scriptRequest.getFilePath() ,ioe);
            throw new ValidationException("Exception received in validation of file existence in github for script  : "
                    + scriptRequest.getFilePath(), ioe);
        }

        validateCephAlias(getCephOutputLocations(scriptRequest.getInputs()));
        validateCephAlias(getCephOutputLocations(scriptRequest.getOutputs()));
    }


    private List<OutputLocation> getCephOutputLocations(Set<ScriptVariable> scriptVariableList) {
        List<OutputLocation> outputLocationList = new ArrayList<>();
        for (ScriptVariable scriptVariable : scriptVariableList) {
            if (scriptVariable.getOutputLocationDetailsList() != null) {
                outputLocationList.addAll(scriptVariable.getOutputLocationDetailsList()
                        .stream()
                        .filter(outputLocation -> outputLocation instanceof CephOutputLocation)
                        .collect(Collectors.toList()));
            }
        }
        return outputLocationList;
    }

    public void validateCephAlias(List<OutputLocation> outputLocationList) throws ValidationException {
        List<String> errorMessages = new ArrayList<>();
        outputLocationList.forEach(outputLocation -> {
                CephOutputLocation cephOutputLocation = (CephOutputLocation) outputLocation;
                try {
                    externalCredentialsActor.getCredentials(cephOutputLocation.getClientAlias());
                } catch (DSPCoreException e) {
                    errorMessages.add("Ceph location alias" + cephOutputLocation.getClientAlias() + " for the given Ceph bucket " + cephOutputLocation.getBucket() +" has not been registered with dsp, " + e.getMessage());
                }
        });
        if (!errorMessages.isEmpty()) {
            throw new ValidationException(errorMessages.toString());
        }
    }
}
