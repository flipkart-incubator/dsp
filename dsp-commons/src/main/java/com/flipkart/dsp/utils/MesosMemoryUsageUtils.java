package com.flipkart.dsp.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.dsp.config.DSPServiceConfig;
import com.flipkart.dsp.entities.misc.WhereClause;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepAudit;
import com.flipkart.dsp.models.PipelineStepStatus;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.flipkart.dsp.utils.Constants.http;
import static com.flipkart.dsp.utils.Constants.questionMark;

/**
 * +
 */
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class MesosMemoryUsageUtils {
    private final DSPServiceConfig.CosmosConfig cosmosConfig;

    public String getMemoryUsageDashboard(Integer attempt, PipelineStepAudit pipelineStepAudit, Map<String, Set<String>> partitionDetails) {
        long startTime = pipelineStepAudit.getCreatedAt().getTime() - TimeUnit.MINUTES.toMillis(2);
        long endTime = new Date().getTime();
        if (pipelineStepAudit.getPipelineStepStatus().equals(PipelineStepStatus.SUCCESS)
                || pipelineStepAudit.getPipelineStepStatus().equals(PipelineStepStatus.FAILED))
            endTime = pipelineStepAudit.getUpdatedAt().getTime();
        endTime += TimeUnit.MINUTES.toMillis(2);

        return http + cosmosConfig.getCosmosEndPoint() + "/dashboard/script/" + cosmosConfig.getCosmosScriptName()
                + questionMark + "appId=" + cosmosConfig.getMesosAppId() + "&requestId=" + pipelineStepAudit.getRefreshId()
                + "&pipelineStepId=" + pipelineStepAudit.getPipelineStepId() + "&attempt=" + attempt
                + "&workflowExecutionId=" + pipelineStepAudit.getWorkflowExecutionId()
                + "&partitionValues=" + getPartitionValues(partitionDetails) + "&from=" + startTime + "&to=" + endTime;
    }

    private String getPartitionValues(Map<String, Set<String>> partitionDetails) {
        StringBuilder partitionValue = new StringBuilder();
        partitionDetails.forEach((key, value1) -> {
            if (key.equals("default_partitions"))
                partitionValue.append("__");
            else {
                value1.forEach(value -> {
                    partitionValue.append("___");
                    partitionValue.append(value);
                    partitionValue.append("___");
                });
            }
        });
        return partitionValue.toString();
    }

    public Map<String, Set<String>> getPartitionDetails(String scope) {
        Map<String, Set<String>> partitionDetails = new HashMap<>();
        TypeReference<LinkedHashSet<WhereClause>> typeRef = new TypeReference<LinkedHashSet<WhereClause>>() {
        };
        LinkedHashSet<WhereClause> scopes = JsonUtils.DEFAULT.fromJson(scope, typeRef);
        if (scopes.isEmpty())
            partitionDetails.put("default_partitions", new HashSet<>());
        scopes.forEach(whereClause -> partitionDetails.put(whereClause.getId(), whereClause.getValues()));
        return partitionDetails;
    }
}
