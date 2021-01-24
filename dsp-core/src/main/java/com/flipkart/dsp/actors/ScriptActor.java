package com.flipkart.dsp.actors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.dsp.dao.ExecutionEnvironmentDAO;
import com.flipkart.dsp.dao.ScriptDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.ScriptEntity;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.script.Script;
import com.flipkart.dsp.exceptions.DSPCoreException;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.utils.JsonUtils;
import lombok.RequiredArgsConstructor;
import org.apache.parquet.Strings;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * +
 */
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ScriptActor implements SGActor<ScriptEntity, Script> {
    private final ScriptDAO scriptDAO;
    private final TransactionLender transactionLender;
    private final ExecutionEnvironmentDAO executionEnvironmentDAO;

    @Override
    public ScriptEntity unWrap(Script script) {
        if (Objects.nonNull(script)) {
            return ScriptEntity.builder().version(script.getVersion())
                    .gitRepo(script.getGitRepo()).gitFolder(script.getGitFolder())
                    .isDraft(script.getIsDraft()).gitFilePath(script.getFilePath()).gitCommitId(script.getGitCommitId())
                    .executionEnvironmentEntity(executionEnvironmentDAO.getEnvironmentEnvironment(script.getExecutionEnvironment()))
                    .inputVariables(JsonUtils.DEFAULT.toJson(script.getInputVariables()))
                    .outputVariables(JsonUtils.DEFAULT.toJson(script.getOutputVariables())).build();
        }
        return null;
    }

    @Override
    public Script wrap(ScriptEntity scriptEntity) {
        if (Objects.nonNull(scriptEntity)) {
            Set<ScriptVariable> inputVariables = getScriptVariables(scriptEntity.getInputVariables());
            Set<ScriptVariable> outputVariables = getScriptVariables(scriptEntity.getOutputVariables());
            return Script.builder().id(scriptEntity.getId()).isDraft(scriptEntity.getIsDraft())
                    .version(scriptEntity.getVersion()).gitRepo(scriptEntity.getGitRepo()).filePath(scriptEntity.getGitFilePath())
                    .gitFolder(scriptEntity.getGitFolder()).gitCommitId(scriptEntity.getGitCommitId())
                    .executionEnvironment(scriptEntity.getExecutionEnvironmentEntity().getExecutionEnvironment())
                    .inputVariables(inputVariables).outputVariables(outputVariables).build();

        }
        return null;
    }

    private Set<ScriptVariable> getScriptVariables(String variableString) {
        if (Strings.isNullOrEmpty(variableString))
            return null;
        else
            return JsonUtils.DEFAULT.fromJson(variableString, new TypeReference<Set<ScriptVariable>>() {});
    }

    public void updateScriptCommitId(Request request,String commitId) {
        if (!Strings.isNullOrEmpty(commitId)) {
            request.getWorkflowDetails().getPipelineSteps().forEach(pipelineStep -> {
                pipelineStep.getScript().setGitCommitId(commitId);
                updateScript(pipelineStep.getScript());
            });
        }
    }

    private void updateScript(Script script) {
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                scriptDAO.persist(unWrap(script));
            }
        });
    }

    public Long save(Script script) {
        AtomicReference<ScriptEntity> atomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(scriptDAO.persist(unWrap(script)));
            }
        });
        return atomicReference.get().getId();
    }

    public Script getScriptById(Long id) {
        AtomicReference<Script> atomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(wrap(scriptDAO.get(id)));
            }
        });
        return atomicReference.get();
    }

    public Script getScriptByGitDetails(String gitRepo, String gitFolder, String gitFilePath, String gitCommitId, boolean isProd) {
        AtomicReference<Script> atomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(wrap(scriptDAO.getLatestScriptByGitDetails(gitRepo, gitFolder, gitFilePath, gitCommitId, isProd)));
            }
        });
        return atomicReference.get();
    }

}
