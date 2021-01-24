package com.flipkart.dsp.validation;

import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exceptions.ValidationException;
import com.flipkart.dsp.models.enums.WorkflowStepStateNotificationType;
import com.flipkart.dsp.models.misc.EmailNotifications;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.flipkart.dsp.models.enums.WorkflowStateNotificationType.FAILURE;
import static com.flipkart.dsp.models.enums.WorkflowStateNotificationType.SUCCESS;
import static java.util.stream.Collectors.toMap;

/**
 * +
 */
public class EmailNotificationsValidator {
    public static void validateEmailNotificationDetails(EmailNotifications emailNotifications, WorkflowDetails workflowDetails) throws ValidationException {
        Map<String /* pipelineStepName*/, List<String> /* input_partitions*/> pipelineStepNamesToPartitionMapping
                = getPipelineStepNameToPartitionsMapping(workflowDetails);
        if (Objects.isNull(emailNotifications))
            return;
        List<String> errorMessages = validateEmailNotificationRecipientsDetails(emailNotifications.getRecipients());
        errorMessages.addAll(validateWorkflowStateForNotifications(emailNotifications.getWorkflowStates()));
        errorMessages.addAll(validatePartitionDetailsMap(pipelineStepNamesToPartitionMapping, emailNotifications.getPartitionDetails()));
        if (errorMessages.size() > 0) {
            String errorMessage = IntStream.range(0, errorMessages.size()).mapToObj(i -> i + 1 + ". " + errorMessages.get(i) + "\n")
                    .collect(Collectors.joining("", "Validation Failed for email Notifications details. Please find the errors:\n", ""));
            throw new ValidationException(errorMessage);
        }
    }


    private static Map<String /* pipelineStepName*/, List<String> /* input_partitions*/> getPipelineStepNameToPartitionsMapping(WorkflowDetails workflowDetails) {
        return workflowDetails.getPipelineSteps().stream().collect(toMap(PipelineStep::getName, PipelineStep::getPartitions));
    }


    private static List<String> validateEmailNotificationRecipientsDetails(Map<String, String> recipients) {
        List<String> errorMessage = new ArrayList<>();
        if (Objects.isNull(recipients) || recipients.size() == 0)
            errorMessage.add("Recipients details can't be null for Email Notifications.");
        else {
            recipients.forEach((recipient, state) -> {
                if (StringUtils.isEmpty(state)) {
                    errorMessage.add("Email Notifications states can't be null for recipient: " + recipient + "." +
                            " Please mention in current format -> [" + recipient + ":" + " success, failure/ success/ failure].");
                } else {
                    List<String> states = Arrays.asList(state.split(","));
                    if (states.size() > 2)
                        errorMessage.add("Email Notifications states can be only success or/and Failure for recipient: " + recipient + ".");
                    else
                        states.forEach(state1 -> validateRecipientState(state1.trim(), recipient, errorMessage));
                }
            });
        }
        return errorMessage;
    }

    private static void validateRecipientState(String state, String recipient, List<String> errorMessages) {
        if (state.equalsIgnoreCase(SUCCESS.toString()) || state.equalsIgnoreCase(FAILURE.toString())) return;
        errorMessages.add("Email Notifications states can be only success or/and failure for recipient: " + recipient);
    }

    private static List<String> validateWorkflowStateForNotifications(String workflowStatesString) {
        List<String> errorMessage = new ArrayList<>();
        if (!StringUtils.isBlank(workflowStatesString)) {
            List<String> workflowStates = Arrays.asList(workflowStatesString.split(","));
            workflowStates.forEach(workflowState -> validateWorkflowState(workflowState.trim(), errorMessage));
        }
        return errorMessage;
    }

    private static void validateWorkflowState(String state, List<String> errorMessages) {
        if (state.equalsIgnoreCase(WorkflowStepStateNotificationType.STARTED.toString())
                || state.equalsIgnoreCase(WorkflowStepStateNotificationType.SG.toString())
                || state.equalsIgnoreCase(WorkflowStepStateNotificationType.WORKFLOW.toString())
                || state.equalsIgnoreCase(WorkflowStepStateNotificationType.OUTPUT_INGESTION.toString()))
            return;
        errorMessages.add("WorkflowEntity state " + state + " is not valid for email Notifications. Possible states are: "
                + "[" + Arrays.stream(WorkflowStepStateNotificationType.values()).map(Enum::toString).collect(Collectors.joining(", ")) + "].");
    }

    private static List<String> validatePartitionDetailsMap(Map<String /* pipelineStepName*/, List<String> /* input_partitions*/> pipelineStepNamesToPartitionMapping,
                                                            Map<String /*pipelineStepName*/, List<Map<String /*partitionKey*/, Object /*partitionValue*/>>> partitionDetailsMap) {
        List<String> errorMessages = new ArrayList<>();
        if (Objects.nonNull(partitionDetailsMap)) {
            partitionDetailsMap.forEach((pipelineStepName, partitionDetailsList) -> {
                boolean isPipelineStepValid = pipelineStepNamesToPartitionMapping.keySet().stream().anyMatch(name -> name.equalsIgnoreCase(pipelineStepName));
                if (isPipelineStepValid) {
                    if (Objects.isNull(partitionDetailsList) || partitionDetailsList.size() == 0)
                        errorMessages.add("partition details can't be null for pipelineStepEntity: " + pipelineStepName + " for Email Notifications.");
                    else
                        partitionDetailsList.forEach(partitionDetails -> validateAllPartitionKeysPresent(pipelineStepName, pipelineStepNamesToPartitionMapping.get(pipelineStepName), partitionDetails, errorMessages));
                } else
                    errorMessages.add("pipeline_step name \"" + pipelineStepName + "\"" + " mentioned in email notifications doesn't exist for any workflowEntity.");
            });
        }
        return errorMessages;
    }


    private static void validateAllPartitionKeysPresent(String pipelineStepName, List<String> inputPartitions,
                                                        Map<String, Object> partitionDetails, List<String> errorMessages) {
        if (Objects.isNull(partitionDetails) || partitionDetails.size() == 0) {
            errorMessages.add("Partition key value details map of pipeline_step \"" + pipelineStepName + "\" are not mentioned for email notifications.");
            return;
        }
        if (!inputPartitions.equals(new ArrayList<>(partitionDetails.keySet())))
            errorMessages.add("Partition Keys of pipeline_step \"" + pipelineStepName + "\"" + " for email notifications are wrong." +
                    " Required values: [" + String.join(", ", inputPartitions) + "]");
    }
}
