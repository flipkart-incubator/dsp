package com.flipkart.dsp.actors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.dsp.dao.ExecutionEnvironmentDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.ExecutionEnvironmentEntity;
import com.flipkart.dsp.db.entities.ExecutionEnvironmentSnapshotEntity;
import com.flipkart.dsp.models.ExecutionEnvironmentSummary;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.utils.JsonUtils;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.Strings;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * cd
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ExecutionEnvironmentActor implements SGActor<ExecutionEnvironmentEntity, ExecutionEnvironmentSummary> {

    private final TransactionLender transactionLender;
    private final ExecutionEnvironmentDAO executionEnvironmentDAO;

    @Override
    public ExecutionEnvironmentEntity unWrap(ExecutionEnvironmentSummary dto) {
        return null;
    }

    @Override
    public ExecutionEnvironmentSummary wrap(ExecutionEnvironmentEntity entity) {
        if (Objects.nonNull(entity)) {
            String imagePath = String.format(Constants.IMAGE_PATH_FORMAT, entity.getDockerHub(),
                    entity.getImageIdentifier(), entity.getImageVersion());
            return ExecutionEnvironmentSummary.builder().id(entity.getId()).name(entity.getExecutionEnvironment())
                    .imagePath(imagePath).specification(getSpecifications(entity)).build();
        }
        return null;
    }

    private ExecutionEnvironmentSummary.Specification getSpecifications(ExecutionEnvironmentEntity executionEnvironment) {
        if (executionEnvironment.getExecutionEnvironmentSnapshotEntities() != null) {
            long maxVersion = Integer.MIN_VALUE;
            for (ExecutionEnvironmentSnapshotEntity executionEnvironmentSnapshot : executionEnvironment.getExecutionEnvironmentSnapshotEntities()) {
                if (executionEnvironmentSnapshot.getVersion() > maxVersion) {
                    List<ExecutionEnvironmentSummary.LanguageLibraries> languageLibraries = getLanguageLibraries(executionEnvironmentSnapshot.getLibrarySet());
                    maxVersion = executionEnvironmentSnapshot.getVersion();
                    List<ExecutionEnvironmentSummary.NativeComponents> nativeComponents = getNativeComponents(executionEnvironmentSnapshot.getNativeLibrarySet());
                    return ExecutionEnvironmentSummary.Specification.builder()
                            .languageLibraries(languageLibraries).nativeComponents(nativeComponents).os(executionEnvironmentSnapshot.getOs())
                            .language(executionEnvironment.getImageLanguage()).osVersion(executionEnvironmentSnapshot.getOsVersion())
                            .languageVersion(executionEnvironment.getImageVersion()).build();
                }
            }
        }
        return null;
    }

    public List<ExecutionEnvironmentSummary> getAllExecutionEnvironments() {
        AtomicReference<List<ExecutionEnvironmentSummary>> atomicReference = new AtomicReference<>();
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(executionEnvironmentDAO.getAllEnvironments().stream()
                        .map(executionEnvironment -> wrap(executionEnvironment)).collect(toList()));
            }
        });
        return atomicReference.get();
    }


    public Set<String> getAllExecutionEnvironmentNames() {
        return getAllExecutionEnvironments().stream().map(ExecutionEnvironmentSummary::getName).collect(toSet());
    }

    public ExecutionEnvironmentSummary getExecutionEnvironmentByName(String executionEnv) {
        return getAllExecutionEnvironments().stream().filter(executionEnvironment ->
                executionEnvironment.getName().equalsIgnoreCase(executionEnv)).findFirst().orElse(null);
    }

    public List<ExecutionEnvironmentSummary> getExecutionEnvironmentsSummary() {
        return getAllExecutionEnvironments();
    }

    public ExecutionEnvironmentSummary getExecutionEnvironmentSummary(String executionEnvironmentName) {
        return getExecutionEnvironmentByName(executionEnvironmentName);
    }


    private List<ExecutionEnvironmentSummary.LanguageLibraries> getLanguageLibraries(String librarySet) {
        if (Strings.isNullOrEmpty(librarySet)) return new ArrayList<>();
        Map<String, String> libraryMap = JsonUtils.DEFAULT.fromJson(librarySet, new TypeReference<Map<String, String>>() {
        });
        return libraryMap.entrySet().stream().map(entry -> ExecutionEnvironmentSummary.LanguageLibraries.builder()
                .name(entry.getKey()).version(entry.getValue()).build()).collect(Collectors.toList());
    }

    private List<ExecutionEnvironmentSummary.NativeComponents> getNativeComponents(String nativeLibrarySet) {
        if (Strings.isNullOrEmpty(nativeLibrarySet)) return new ArrayList<>();
        Map<String, Object> nativeLibraryMap = JsonUtils.DEFAULT.fromJson(nativeLibrarySet, new TypeReference<Map<String, Object>>() {
        });

        return nativeLibraryMap.entrySet().stream().map(entry -> ExecutionEnvironmentSummary.NativeComponents.builder()
                .name(entry.getKey()).architecture(((LinkedHashMap) entry.getValue()).get("architecture").toString())
                .version(((LinkedHashMap) entry.getValue()).get("version").toString()).build()).collect(Collectors.toList());
    }

    public ExecutionEnvironmentSummary getExecutionEnvironmentById(Long id) {
        AtomicReference<ExecutionEnvironmentEntity> atomicReference = new AtomicReference<>();
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(executionEnvironmentDAO.get(id));
            }
        });
        return wrap(atomicReference.get());
    }
}
