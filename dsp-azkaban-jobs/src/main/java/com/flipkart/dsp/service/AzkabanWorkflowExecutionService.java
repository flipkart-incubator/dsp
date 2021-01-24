package com.flipkart.dsp.service;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.actors.RequestActor;
import com.flipkart.dsp.dao.DataFrameAuditDAO;
import com.flipkart.dsp.dao.WorkflowAuditDAO;
import com.flipkart.dsp.dao.WorkflowDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.*;
import com.flipkart.dsp.entities.misc.ConfigPayload;
import com.flipkart.dsp.entities.misc.WhereClause;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.sg.core.DataFrameAuditStatus;
import com.flipkart.dsp.entities.sg.dto.*;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exceptions.*;
import com.flipkart.dsp.mesos.WorkflowMesosExecutionDriver;
import com.flipkart.dsp.utils.DataframeSizeExtractor;
import com.flipkart.dsp.utils.JsonUtils;
import com.flipkart.dsp.validation.Validator;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.javatuples.Triplet;

import javax.validation.constraints.NotNull;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.flipkart.dsp.entities.sg.dto.DataFrameColumnType.IN;
import static com.flipkart.dsp.utils.DataframeUtils.getReferenceDFForParallelism;
import static java.util.Objects.isNull;
import static java.util.stream.Collectors.*;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.join;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class AzkabanWorkflowExecutionService implements WorkflowExecutionService {

    private final Validator validator;
    private final RequestActor requestActor;
    private final WorkflowDAO workflowDAO;
    private final WorkflowAuditDAO workflowAuditDAO;
    private final TransactionLender transactionLender;
    private final DataFrameAuditDAO dataFrameAuditDAO;
    private final DataframeSizeExtractor dataframeSizeExtractor;
    private final WorkflowMesosExecutionDriver workflowMesosExecutionDriver;

    @Override
    public boolean executeWorkflow(Request request, WorkflowDetails workflowDetails, SGJobOutputPayload payload,
                                   String workflowExecutionId, String mesosQueue, Boolean failFast,
                                   PipelineStep pipelineStep) throws DSPCoreException {
        if (workflowDetails.getWorkflow().getParentWorkflowId() == null) {
            return generateAndPushPayloads(request, workflowDetails, true,
                    payload.getSgUseCasePayloadSet(), workflowExecutionId, mesosQueue, failFast, pipelineStep);
        } else {
            return generateAndPushPayloads(request, workflowDetails, false,
                    payload.getSgUseCasePayloadSet(), workflowExecutionId, mesosQueue, failFast, pipelineStep);
        }
    }

    public boolean executeSG(Long requestId, WorkflowDetails workflowDetails, PipelineStep pipelineStep,
                             String workflowExecutionId, String mesosQueue, Boolean failFast) throws DSPCoreException {

        return workflowMesosExecutionDriver.executeSG(workflowDetails, 0,
                mesosQueue, requestId, pipelineStep.getId(), workflowExecutionId, pipelineStep);

    }

    /**
     * Generate ConfigPayload,WorkflowAuditEntity pair. ConfigPayload is all the information required to
     * run a workflow, including dataframe locations and pipelines to run. And trigger the workflow
     ***/
    @Timed
    @Metered
    private boolean generateAndPushPayloads(Request request, WorkflowDetails workflowDetails,
                                            boolean training, Set<SGUseCasePayload> sgUseCasePayload,
                                            String workflowExecutionId, String mesosQueue, Boolean failFast,
                                            PipelineStep pipelineStep) throws DSPCoreException {

        AtomicReference<List<Triplet<ConfigPayload, Long, Long>>> reference = new AtomicReference<>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() throws ValidationException {
                reference.set(prepareWorkflowExecutionPayloads(request, workflowDetails, training,
                        sgUseCasePayload, workflowExecutionId, pipelineStep));
            }
        }, "preparation of workflow Execution payloads Failed.");
        return workflowMesosExecutionDriver.execute(workflowDetails, workflowDetails.getWorkflow().getRetries(),
                mesosQueue, reference.get(), failFast, request.getId(), pipelineStep);
    }

    private List<Triplet<ConfigPayload, Long, Long>> prepareWorkflowExecutionPayloads(Request request,
                                                                                      WorkflowDetails workflowDetails,
                                                                                      boolean training, Set<SGUseCasePayload> sgUseCasePayload,
                                                                                      String workflowExecutionId,
                                                                                      PipelineStep pipelineStep) throws PartitionCountMismatchException, ValidationException {
        List<ConfigPayload> payloadList = generatePayloadsForWorkFlow(request, workflowDetails,
                training, sgUseCasePayload, workflowExecutionId, pipelineStep);
        return dataframeSizeExtractor.calculateDFSize(payloadList);
    }

    private List<ConfigPayload> generatePayloadsForWorkFlow(Request request, WorkflowDetails workflowDetails, boolean isTrainingWorkFlow,
                                                            Set<SGUseCasePayload> sgUseCasePayload, String workflowExecutionId, PipelineStep pipelineStep) throws PartitionCountMismatchException, ValidationException {
        final Workflow workflow = workflowDetails.getWorkflow();
        WorkflowEntity parentWorkflow = workflow.getParentWorkflowId() == null ? null : workflowDAO.get(workflow.getParentWorkflowId());
        RequestEntity parentRequest = getParentRequest(workflow.getParentWorkflowId(), request.getWorkflowDetails().getParentWorkflowRefreshId());
        String parentWorkflowExecutionId = getParentPipelineExecutionId(parentRequest);
        List<SGUseCasePayload> currentWorkFlowPayloads = sgUseCasePayload.stream().map(this::enrichAndSanitiseUseCasePayload).collect(toList());
        List<SGUseCasePayload> trainingPayloads = null;
        if (!isTrainingWorkFlow) {
            if (Objects.isNull(parentWorkflow)) throw new ValidationException("No training workflow found for Training workflowId: " + workflow.getParentWorkflowId());
            if (Objects.isNull(parentRequest)) throw new ValidationException("No completed request found for training workflow.");
            trainingPayloads = parentWorkflow.getDataFrames().stream().map(dataFrameEntity -> getLatestDataframeAuditPayload(dataFrameEntity, parentRequest))
                    .map(this::enrichAndSanitiseUseCasePayload).collect(toList());
        }

        List<AzkabanWorkflowExecutionService.PartitionedDFLocation> partitionedDFList =
                distributeWorkLoad(trainingPayloads, currentWorkFlowPayloads, isTrainingWorkFlow, workflow.getName(), pipelineStep);

        List<List<ConfigPayload>> configPayloadAndWorkFlowAudits =
                partitionedDFList.stream().map(d -> constructTopicToPayloadMap(request.getId(), workflow.getId(),
                        workflowExecutionId, parentWorkflowExecutionId, d.getScope(),
                        d.getCsvLocations(), d.getFutureCSVLocations(), d.getPartitionValues(), pipelineStep))
                        .collect(toList());

        return configPayloadAndWorkFlowAudits.stream().flatMap(Collection::stream).collect(toList());
    }

    private String getParentPipelineExecutionId(RequestEntity parentRequest) throws ValidationException {
        if (Objects.isNull(parentRequest))
            return null;
        List<WorkflowAuditEntity> workflowAudits = workflowAuditDAO.getWorkflowAuditsByRequestId(parentRequest.getId());
        if (workflowAudits.size() == 0)
            throw new ValidationException("No Successful workflow audit found for refresh_id: " + parentRequest.getId());
        return workflowAudits.get(0).getWorkflowExecutionId();
    }

    private RequestEntity getParentRequest(Long parentWorkflowId, Long parentWorkflowRefreshId) throws ValidationException {
        if (parentWorkflowId != null) {
            if (Objects.nonNull(parentWorkflowRefreshId)) {
                RequestEntity request = requestActor.getLatestSuccessfulRequest(parentWorkflowRefreshId);
                validator.validateParentWorkflowRefreshId(request, parentWorkflowRefreshId, parentWorkflowId);
                return request;
            } else {
                return validator.validateLatestSuccessRequest(parentWorkflowId);
            }
        } else return null;
    }

    /**
     * Seggregate the dataframes by partition so that they can be processed parallely
     **/
    private List<AzkabanWorkflowExecutionService.PartitionedDFLocation> distributeWorkLoad(
            List<SGUseCasePayload> trainingPayloads, List<SGUseCasePayload> currentWorkFlowPayloads, boolean isTrainingWorkFlow,
            String workflowName, PipelineStep pipelineStep) throws PartitionCountMismatchException {
        List<String> partitions = pipelineStep.getPartitions();
        SGUseCasePayload referencePayload = getReferenceDFForParallelism(workflowName, partitions, currentWorkFlowPayloads);
        Set<List<DataFrameKey>> dataFrameKeys = referencePayload.getDataframes().keySet();

        List<PartitionedDFLocation> list = new ArrayList<>();
        Set<List<DataFrameKey>> missingKeys = new HashSet<>();
        for (List<DataFrameKey> dfk : dataFrameKeys) {
            try {
                PartitionedDFLocation dfLocationsForPartitionScope = getDFLocationsForPartitionScope(dfk, trainingPayloads,
                        currentWorkFlowPayloads,
                        isTrainingWorkFlow, partitions);
                list.add(dfLocationsForPartitionScope);
            } catch (PartitionCountMismatchException e) {
                missingKeys.add(dfk);
            }
        }
        float mismatchThreshold = (float) missingKeys.size() / (float) dataFrameKeys.size();
        double MISMATCH_THRESHOLD = 0.7;
        if (mismatchThreshold > MISMATCH_THRESHOLD) {
            throw new PartitionCountMismatchException("More than 50% of the Reference partitions did not get corresponding partitions from other dataframes.");
        }
        return list;
    }

    private AzkabanWorkflowExecutionService.PartitionedDFLocation
    getDFLocationsForPartitionScope(List<DataFrameKey> dataFrameKeys, List<SGUseCasePayload> trainingPayloads,
                                    List<SGUseCasePayload> currentWorkFlowPayloads, boolean isTrainingWorkFlow,
                                    List<String> partitions) throws PartitionCountMismatchException {
        String scope = getScope(dataFrameKeys);
        Map<String, String> partitionValues = getPartitionValues(dataFrameKeys);
        Map<String, String> trainingDFLocations = getTrainingDataFrameLocations(dataFrameKeys, trainingPayloads, scope, partitions);

        Map<String, String> currentWFDFLocations = getExecutionDataframeLocations(dataFrameKeys, currentWorkFlowPayloads, scope, partitions);

        Map<String, String> csvLocations = isTrainingWorkFlow ? currentWFDFLocations : trainingDFLocations;
        Map<String, String> futureCsvLocations = isTrainingWorkFlow ? new HashMap<>() : currentWFDFLocations;

        return AzkabanWorkflowExecutionService.PartitionedDFLocation.builder().scope(scope).csvLocations(csvLocations)
                .futureCSVLocations(futureCsvLocations).partitionValues(partitionValues).build();
    }

    private Map<String, String>
    getExecutionDataframeLocations(List<DataFrameKey> dataFrameKeys, List<SGUseCasePayload> currentWorkFlowPayloads,
                                   String scope, List<String> partitionList) throws PartitionCountMismatchException {
        Map<String, String> currentWFDFLocations;
        try {
            currentWFDFLocations = getDataFrameLocations(dataFrameKeys, currentWorkFlowPayloads, partitionList);
            validateWorkflowDataFrames(scope, currentWFDFLocations);
        } catch (Exception e) {
            final Map<String, Integer> partitionCountMap = currentWorkFlowPayloads.stream()
                    .collect(toMap(SGUseCasePayload::getDataFrameId, sp -> sp.getDataframes() == null ? 0 : sp.getDataframes().size()));
            throw new PartitionCountMismatchException("  Number of partitions don't match for one of the data frames. partitionCountMap: " + partitionCountMap, e);
        }
        return currentWFDFLocations;
    }

    private Map<String, String> getPartitionValues(List<DataFrameKey> dataFrameKeys) {
        Map<String, String> partitionValues = new HashMap<>();
        for (DataFrameKey dataFrameKey : dataFrameKeys) {
            String dataFrameKeyScope = JsonUtils.DEFAULT.toJson(((DataFrameMultiKey) dataFrameKey).getValues());
            dataFrameKeyScope = dataFrameKeyScope.replaceAll("%23", "_").replaceAll(" ", "_");
            partitionValues.put(dataFrameKey.getName(), dataFrameKeyScope);
        }
        return partitionValues;
    }


    private Map<String, String> getTrainingDataFrameLocations(List<DataFrameKey> dataFrameKeys, List<SGUseCasePayload> trainingPayloads,
                                                              String scope, List<String> partitions) throws PartitionCountMismatchException {
        if (Objects.isNull(trainingPayloads) || trainingPayloads.size() == 0) return new HashMap<>();
        try {
            Map<String, String> trainingDFLocations = getDataFrameLocations(dataFrameKeys, trainingPayloads, partitions);
            validateTrainingDataFrames(scope, trainingDFLocations);
            return trainingDFLocations;
        } catch (Exception e) {
            final Map<String, Integer> partitionCountMap = trainingPayloads.stream().collect(toMap(SGUseCasePayload::getDataFrameId,
                    sp -> sp.getDataframes() == null ? 0 : sp.getDataframes().size()));
            throw new PartitionCountMismatchException("Number of partitions don't match for one of" +
                    " the training data frames. partitionCountMap: " + partitionCountMap, e);
        }
    }

    private Map<String, String> getDataFrameLocations(List<DataFrameKey> dataFrameKeys,
                                                      List<SGUseCasePayload> sgUseCasePayloads, List<String> partitions) {
        Map<String, String> map = new HashMap<>();
        for (SGUseCasePayload p : sgUseCasePayloads) {
            if (map.put(p.getDataFrameId(), getDFLocationForPartitionScope(dataFrameKeys, p, partitions)) != null) {
                throw new IllegalStateException("Duplicate key");
            }
        }
        return map;
    }

    private void validateWorkflowDataFrames(String scope, Map<String, String> currentWFDFLocations) {
        currentWFDFLocations.forEach((k, v) -> {
            if (isBlank(v)) {
                log.error("Dataframe does not exist for partition : {}", scope);
                throw new EntityNotFoundException(DataFrameAuditEntity.class.getSimpleName(), "Dataframe does not exist for partition!!");
            }
        });
    }

    private void validateTrainingDataFrames(String scope, Map<String, String> trainingDFLocations) {
        trainingDFLocations.forEach((k, v) -> {
            if (isBlank(v)) {
                log.error("Training dataframe does not exist for partition : {}", scope);
                throw new EntityNotFoundException(DataFrameAuditEntity.class.getSimpleName(), "Training dataframe does not exist for partition!!");
            }
        });
    }

    /**
     * Get the location of csv for a given partition scope in a dataframe
     **/
    private String getDFLocationForPartitionScope(List<DataFrameKey> refDataFrameKeys,
                                                  SGUseCasePayload payload,
                                                  List<String> partitionList) {

        Set<String> payloadPartitionKeys = payload.getColumnMetaData().keySet();
        List<String> matchingPartitions = partitionList.stream().filter(p -> payloadPartitionKeys.contains(p)).collect(toList());

        Map<List<DataFrameKey>, Set<String>> dataframes = payload.getDataframes();
        if (matchingPartitions.isEmpty() && dataframes.size() > 1) {
            log.error("Data Frame Partitioned while it was expected NOT to !!! DataFrame ID : {}",
                    payload.getDataFrameId());
            throw new RuntimeException("Data Frame Partitioned while it was expected NOT to !!!");
        }

        Optional<String> locationOptional = dataframes.entrySet().stream()
                .filter(e -> doesPartitionsMatch(matchingPartitions, refDataFrameKeys, e.getKey()))
                .map(Map.Entry::getValue)
                .findFirst()
                .map(set -> join(set, ","));
        if (!locationOptional.isPresent())
            throw new MissingPartitionException("Partition not found for partition key: "
                    + refDataFrameKeys.iterator().next() + " For dataframe: " + payload.getDataFrameId());
        return locationOptional.get();
    }

    private boolean doesPartitionsMatch(List<String> matchingPartitions, List<DataFrameKey> refKeys,
                                        List<DataFrameKey> dataFrameKeys) {
        Map<String, DataFrameKey> dataFrameKeyMap = dataFrameKeys.stream().collect(toMap(DataFrameKey::getName, d -> d));

        boolean isNotMatching = refKeys.stream().anyMatch(refKey -> {
            boolean isPartitionKey = matchingPartitions.contains(refKey.getName());
            boolean isdataFrameKeyMatching = refKey.equals(dataFrameKeyMap.get(refKey.getName()));
            return isPartitionKey && !isdataFrameKeyMatching;
        });

        return !isNotMatching;
    }

    private String getScope(List<DataFrameKey> dataFrameKeys) {
        LinkedHashSet<WhereClause>
                whereClauses =
                dataFrameKeys.stream().map(dfk -> {
                    WhereClause whereClause = new WhereClause();
                    whereClause.setId(dfk.getName());
                    if (IN.equals(dfk.getColumnType())) {
                        whereClause.setValues(((DataFrameMultiKey) dfk).getValues());
                    }
                    return whereClause;
                }).collect(toCollection(LinkedHashSet::new));

        return JsonUtils.DEFAULT.toJson(whereClauses);
    }

    private SGUseCasePayload enrichAndSanitiseUseCasePayload(SGUseCasePayload payload) {
        payload.addColumnNameToDataFrameKeys();

        return removeNonPartitionDataFromDFKeys(payload);
    }

    private SGUseCasePayload getLatestDataframeAuditPayload(DataFrameEntity dataFrame, RequestEntity requestEntity) {
        List<DataFrameAuditEntity> successfulDataFrameAudits = requestEntity.getRequestDataframeAudits().stream().filter(requestDataframeAuditEntity ->
                requestDataframeAuditEntity.getDataFrameAuditEntity().getDataFrameEntity().getId().equals(dataFrame.getId())
                        && requestDataframeAuditEntity.getDataFrameAuditEntity().getStatus().equals(DataFrameAuditStatus.COMPLETED)).collect(toList()).stream()
                .map(RequestDataframeAuditEntity::getDataFrameAuditEntity).collect(Collectors.toList());

        if (successfulDataFrameAudits.size() == 0)
            throw new EntityNotFoundException(DataFrameAuditEntity.class.getSimpleName(), "Training dataframe not found for Dataframe : " + dataFrame.getId());

        successfulDataFrameAudits.sort(Comparator.comparing(DataFrameAuditEntity::getRunId).reversed());
        SGUseCasePayload useCasePayload = successfulDataFrameAudits.get(0).getPayload();

        if (isNull(useCasePayload))
            throw new IllegalStateException("Training dataframe does not have output payload");

        return useCasePayload;
    }

    private List<ConfigPayload> constructTopicToPayloadMap(Long refreshId, long workflowId,
                                                           String workflowExecutionId, String parentWorkflowExecutionId,
                                                           String scope, Map<String, String> csvLocation,
                                                           Map<String, String> futureCSVLocation,
                                                           Map<String, String> partitionValues,
                                                           PipelineStep currentPipelineStep) {
        final String pipelineExecutionId = UUID.randomUUID().toString();
        List<PipelineStep> orderPipelineSteps = new ArrayList<>();
        orderPipelineSteps.add(currentPipelineStep);
        List<String> partitionKeys = new ArrayList<>(partitionValues.keySet());

        return orderPipelineSteps.stream()
                .filter(pipelineStep -> pipelineStep.getPartitions().size() == partitionKeys.size()
                        && pipelineStep.getPartitions().containsAll(partitionKeys)
                        && partitionKeys.containsAll(pipelineStep.getPartitions()))
                .map(pipelineStep -> buildConfigPayload(workflowId, workflowExecutionId, parentWorkflowExecutionId,
                        scope, csvLocation, futureCSVLocation, partitionValues, pipelineExecutionId,
                        refreshId, pipelineStep)).collect(toList());
    }

    private ConfigPayload buildConfigPayload(Long workflowId, String workflowExecutionId, String parentWorkflowExecutionId,
                                             String scope, Map<String, String> csvLocation, Map<String, String> futureCSVLocation,
                                             Map<String, String> partitionValues, String pipelineExecutionId,
                                             long refreshId1, PipelineStep pipelineStep) {
        return ConfigPayload.builder().
                workflowId(workflowId).
                scope(scope).
                csvLocation(csvLocation).
                futureCSVLocation(futureCSVLocation).
                pipelineStepId(pipelineStep.getId()).
                timestamp(Instant.now().toString()).
                workflowExecutionId(workflowExecutionId).
                parentWorkflowExecutionId(parentWorkflowExecutionId).
                pipelineExecutionId(pipelineExecutionId).
                refreshId(refreshId1).
                partitionValues(partitionValues).
                build();
    }

    /**
     * Iterate through each dataframe (partition), and remove the DataFrameKeys which are not partitions
     **/
    SGUseCasePayload removeNonPartitionDataFromDFKeys(SGUseCasePayload sgUseCasePayload) {

        Map<List<DataFrameKey>, Set<String>> dFsWithOnlyPartitionData = Maps.newHashMap();

        int index = -1;
        ArrayList<Integer> addColumns = new ArrayList<>();
        for (Map.Entry<String, DataFrameColumnType> key : sgUseCasePayload.getColumnMetaData().entrySet()) {
            index++;
            if (sgUseCasePayload.getColumnMetaData().containsKey(key.getKey()))
                addColumns.add(index);
        }
        for (Map.Entry<List<DataFrameKey>, Set<String>> dataframe : sgUseCasePayload.getDataframes().entrySet()) {
            List<DataFrameKey> dataFrameKeySet = new ArrayList<>();
            int iteration = -1;
            for (DataFrameKey dataframekey : dataframe.getKey()) {
                iteration++;
                if (addColumns.contains(iteration))
                    dataFrameKeySet.add(dataframekey);
            }
            dFsWithOnlyPartitionData.put(dataFrameKeySet, dataframe.getValue());
        }
        return new SGUseCasePayload(sgUseCasePayload.getRequestId(), sgUseCasePayload.getDataFrameId(),
                sgUseCasePayload.getColumnMetaData(), dFsWithOnlyPartitionData);
    }


    @Data
    @Builder
    private static class PartitionedDFLocation {

        @NotNull
        private String scope;

        private Map</** DF ID **/String, /** partition Directory **/String> csvLocations;

        private Map</** DF ID **/String, /** partition Directory **/String> futureCSVLocations;

        private Map<String, String> partitionValues;
    }
}
