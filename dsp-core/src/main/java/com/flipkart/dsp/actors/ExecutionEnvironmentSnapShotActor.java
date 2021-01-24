package com.flipkart.dsp.actors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.dsp.dao.ExecutionEnvironmentDAO;
import com.flipkart.dsp.dao.ExecutionEnvironmentSnapshotDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.ExecutionEnvironmentEntity;
import com.flipkart.dsp.db.entities.ExecutionEnvironmentSnapshotEntity;
import com.flipkart.dsp.entities.misc.ImageDetail;
import com.flipkart.dsp.exceptions.DSPCoreException;
import com.flipkart.dsp.models.ExecutionEnvironmentSnapshot;
import com.flipkart.dsp.utils.JsonUtils;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * cd
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ExecutionEnvironmentSnapShotActor implements SGActor<ExecutionEnvironmentSnapshotEntity, ExecutionEnvironmentSnapshot> {

    private final TransactionLender transactionLender;
    private final ExecutionEnvironmentDAO executionEnvironmentDAO;
    private final ExecutionEnvironmentSnapshotDAO executionEnvironmentSnapShotDAO;

    @Override
    public ExecutionEnvironmentSnapshotEntity unWrap(ExecutionEnvironmentSnapshot executionEnvironmentSnapshot) {
        if (Objects.nonNull(executionEnvironmentSnapshot)) {
            return ExecutionEnvironmentSnapshotEntity.builder()
                    .os(executionEnvironmentSnapshot.getOs()).osVersion(executionEnvironmentSnapshot.getOsVersion())
                    .version(executionEnvironmentSnapshot.getVersion()).createdAt(new Timestamp(System.currentTimeMillis()))
                    .librarySet(executionEnvironmentSnapshot.getLibrarySet()).nativeLibrarySet(executionEnvironmentSnapshot.getNativeLibrarySet())
                    .imageLatestDigest(executionEnvironmentSnapshot.getLatestImageDigest())
                    .languageVersion(executionEnvironmentSnapshot.getImageLanguageVersion())
                    .executionEnvironmentId(executionEnvironmentSnapshot.getExecutionEnvironmentId()).build();
        }
        return null;
    }

    @Override
    public ExecutionEnvironmentSnapshot wrap(ExecutionEnvironmentSnapshotEntity executionEnvironmentSnapshotEntity) {
        if (Objects.nonNull(executionEnvironmentSnapshotEntity)) {
            return ExecutionEnvironmentSnapshot.builder().version(executionEnvironmentSnapshotEntity.getVersion())
                    .librarySet(executionEnvironmentSnapshotEntity.getLibrarySet())
                    .latestImageDigest(executionEnvironmentSnapshotEntity.getImageLatestDigest())
                    .executionEnvironmentId(executionEnvironmentSnapshotEntity.getExecutionEnvironmentId())
                    .nativeLibrarySet(executionEnvironmentSnapshotEntity.getNativeLibrarySet())
                    .os(executionEnvironmentSnapshotEntity.getOs()).osVersion(executionEnvironmentSnapshotEntity.getOsVersion())
                    .imageLanguageVersion(executionEnvironmentSnapshotEntity.getLanguageVersion()).build();
        }
        return null;
    }

    public List<ExecutionEnvironmentSnapshotEntity> unWrap(List<ExecutionEnvironmentSnapshot> executionEnvironmentSnapshots) {
        return executionEnvironmentSnapshots.stream().map(this::unWrap).collect(Collectors.toList());
    }


    public List<ExecutionEnvironmentSnapshot> wrap(List<ExecutionEnvironmentSnapshotEntity> executionEnvironmentSnapshotEntities) {
        return executionEnvironmentSnapshotEntities.stream().map(this::wrap).collect(Collectors.toList());
    }

    public ExecutionEnvironmentSnapshot getLatestExecutionEnvironmentSnapShot(Long executionEnvironmentId) {
        List<ExecutionEnvironmentSnapshotEntity> executionEnvironmentSnapshotEntities = executionEnvironmentSnapShotDAO
                .getExecutionEnvironmentSnapshots(executionEnvironmentId);

        Optional<ExecutionEnvironmentSnapshotEntity> latestExecutionEnvironmentSnapShotEntity = executionEnvironmentSnapshotEntities.stream()
                .max(Comparator.comparing(ExecutionEnvironmentSnapshotEntity::getVersion));
        if (!latestExecutionEnvironmentSnapShotEntity.isPresent())
            return wrap(ExecutionEnvironmentSnapshotEntity.builder().build());
        return wrap(latestExecutionEnvironmentSnapShotEntity.get());
    }

    public List<ExecutionEnvironmentSnapshot> getAllSnapShots(Long executionEnvironmentId) {
        List<ExecutionEnvironmentSnapshotEntity> executionEnvironmentSnapshotEntities = executionEnvironmentSnapShotDAO
                .getExecutionEnvironmentSnapshots(executionEnvironmentId);
        return executionEnvironmentSnapshotEntities.stream().map(this::wrap).collect(Collectors.toList());
    }

    public List<ImageDetail> getImageDetails() {
        AtomicReference<List<ImageDetail>> atomicReference = new AtomicReference<>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(convertToImageDetails(executionEnvironmentDAO.getAllEnvironments()));
            }
        });
        return atomicReference.get();
    }

    private List<ImageDetail> convertToImageDetails(List<ExecutionEnvironmentEntity> executionEnvironmentEntities) {
        return executionEnvironmentEntities.stream().map(entity -> ImageDetail.builder()
                .imageLanguage(entity.getImageLanguage()).imageName(entity.getImageIdentifier())
                .librarySet(getLibrarySet(entity.getExecutionEnvironmentSnapshotEntities())).build()).collect(toList());
    }

    private Map<String, String> getLibrarySet(List<ExecutionEnvironmentSnapshotEntity> executionEnvironmentSnapshotEntities) {
        executionEnvironmentSnapshotEntities.sort(Comparator.comparing(ExecutionEnvironmentSnapshotEntity::getVersion));
        if (executionEnvironmentSnapshotEntities.size() == 0)
            return Maps.newHashMap();
        return JsonUtils.DEFAULT.fromJson(executionEnvironmentSnapshotEntities.get(0).getLibrarySet(),
                new TypeReference<Map<String, String>>() {
                });
    }

    public Long save(ExecutionEnvironmentSnapshot executionEnvironmentSnapShot) throws DSPCoreException {
        AtomicReference<ExecutionEnvironmentSnapshotEntity> optionalAtomicReference = new AtomicReference<>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                optionalAtomicReference.set(executionEnvironmentSnapShotDAO.persist(unWrap(executionEnvironmentSnapShot)));
            }
        }, "Creation of Execution Environment SnapShot unsuccessful.");
        return optionalAtomicReference.get().getId();
    }
}
