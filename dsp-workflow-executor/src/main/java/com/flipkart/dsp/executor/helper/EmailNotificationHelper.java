package com.flipkart.dsp.executor.helper;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.config.DSPClientConfig;
import com.flipkart.dsp.entities.misc.ConfigPayload;
import com.flipkart.dsp.entities.misc.NotificationPreference;
import com.flipkart.dsp.entities.misc.WhereClause;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.models.enums.WorkflowStateNotificationType;
import com.flipkart.dsp.models.misc.EmailNotifications;
import com.flipkart.dsp.models.misc.PartitionDetailsEmailNotificationRequest;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.utils.JsonUtils;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

import static com.flipkart.dsp.utils.Constants.colon;
import static com.flipkart.dsp.utils.Constants.slash;

/**
 * +
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class EmailNotificationHelper {
    private final DSPClientConfig dspClientConfig;
    private final DSPServiceClient dspServiceClient;

    public void sendNotifications(Long workflowId, Long pipelineStepAuditId, ConfigPayload configPayload,
                                  WorkflowStateNotificationType workflowStateNotificationType) {
        try{
            Long refreshId = configPayload.getRefreshId();
            Request request = dspServiceClient.getRequest(refreshId);
            String logs = getLogUrl(pipelineStepAuditId);
            Map<String, Object> partitionDetails = getPartitionDetailsForEmail(workflowId, request, configPayload);
            if (partitionDetails.size() != 0) {
                PartitionDetailsEmailNotificationRequest partitionDetailsEmailNotificationRequest = PartitionDetailsEmailNotificationRequest.builder()
                        .logs(logs).requestId(request.getId()).workflowId(workflowId).partitionDetails(partitionDetails)
                        .workflowStateNotificationType(workflowStateNotificationType).build();
                dspServiceClient.sendEmailNotificationForPartitionStateChangeRequest(partitionDetailsEmailNotificationRequest);
            }
        } catch (Exception e) {
            log.error("Failed to send Email notification!");
        }
    }

    private Map<String, Object> getPartitionDetailsForEmail(Long workflowId, Request request, ConfigPayload configPayload) {
        TypeReference<List<WhereClause>> listTypeReference = new TypeReference<List<WhereClause>>() {};
        List<WhereClause> whereClauses = JsonUtils.DEFAULT.fromJson(configPayload.getScope(), listTypeReference);
        PipelineStep pipelineStep = dspServiceClient.getPipelineStepById(configPayload.getPipelineStepId());
        Map<String, List<Map<String, Object>>> partitionDetailsListFromNotifications = getPartitionDetailsListFromNotifications(workflowId, request);
        List<Map<String, Object>> partitionDetailsForStep = getPartitionDetailsForStep(pipelineStep, partitionDetailsListFromNotifications);

        for (Map<String, Object> partitionDetailForStep : partitionDetailsForStep)
            if (whereClauses.stream().allMatch(whereClause -> partitionDetailForStep.containsKey(whereClause.getId())
                    && Objects.nonNull(whereClause.getValues()) && whereClause.getValues().contains(partitionDetailForStep.get(whereClause.getId()).toString())))
                return partitionDetailForStep;
        return new HashMap<>();
    }

    private List<Map<String, Object>> getPartitionDetailsForStep(PipelineStep pipelineStep, Map<String, List<Map<String, Object>>> partitionDetailsListFromNotifications) {
        return partitionDetailsListFromNotifications.entrySet().stream().
                filter(entry -> pipelineStep.getName().equalsIgnoreCase(entry.getKey())).findFirst().map(Map.Entry::getValue).orElse(new ArrayList<>());
    }

    private Map<String, List<Map<String, Object>>> getPartitionDetailsListFromNotifications(Long workflowId, Request request) {
        if (Objects.nonNull(request.getData()) && verifyPartitionDetails(request.getData().getEmailNotifications()))
            return request.getData().getEmailNotifications().getPartitionDetails();
        else {
            NotificationPreference notificationPreference = dspServiceClient.getNotificationPreference(workflowId);
            if (Objects.nonNull(notificationPreference) && verifyPartitionDetails(notificationPreference.getEmailNotificationPreferences()))
                return notificationPreference.getEmailNotificationPreferences().getPartitionDetails();
            return new HashMap<>();
        }
    }

    private boolean verifyPartitionDetails(EmailNotifications emailNotifications) {
        return Objects.nonNull(emailNotifications) && Objects.nonNull(emailNotifications.getPartitionDetails());
    }

    private String getLogUrl(Long pipelineStepAuditId)  {
        return Constants.http + dspClientConfig.getHost() + colon + dspClientConfig.getPort() + Constants.LOG_RESOURCE_PREFIX
                + slash + pipelineStepAuditId + slash + "log";
    }

}
