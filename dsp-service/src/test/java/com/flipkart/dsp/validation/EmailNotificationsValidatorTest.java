//package com.flipkart.dsp.validation;
//
//import com.flipkart.dsp.models.misc.EmailNotifications;
//import org.junit.Test;
//
//import java.util.*;
//
//import static org.junit.Assert.assertTrue;
//
///**
// * +
// */
//public class EmailNotificationsValidatorTest {
//
//    private String workflowStates = "STARTED, SG, WORKFLOW, OUTPUT_INGESTION";
//
//    private Set<String> inputPartitions = new HashSet<>();
//    private Map<String, String> recipients = new HashMap<>();
//    private Map<String, Object> partitionDetailsStep2 = new HashMap<>();
//    private List<Map<String, Object>> partitionDetailsListStep1 = new ArrayList<>();
//    private List<Map<String, Object>> partitionDetailsListStep2 = new ArrayList<>();
//    private Map<String /* pipelineStepName*/, Set<String> /* input_partitions*/> pipelineStepNamesToPartitionMapping = new HashMap<>();
//    private Map<String /*pipelineStepName*/, List<Map<String /*partitionKey*/, Object /*partitionValue*/>>> partitionDetailsMap = new HashMap<>();
//
//
//    @Test
//    public void testValidateEmailNotificationDetailsFailureCase1() {
//        partitionDetailsMap.put("pipelineStep1", null);
//        partitionDetailsMap.put("pipelineStep2", null);
//        pipelineStepNamesToPartitionMapping.put("pipelineStep1", inputPartitions);
//        EmailNotifications emailNotifications = EmailNotifications.builder().recipients(recipients)
//                .workflowStates("Invalid").partitionDetails(partitionDetailsMap).build();
//
//        boolean isException = false;
//        try {
//            EmailNotificationsValidator.validateEmailNotificationDetails(emailNotifications, pipelineStepNamesToPartitionMapping);
//        } catch (Exception e) {
//            isException = true;
//            List<String> errorMessages = Arrays.asList(e.getMessage().split("\n"));
//            assertTrue(errorMessages.get(1).contains("Recipients details can't be null for Email Notifications."));
//            assertTrue(errorMessages.get(2).contains("WorkflowEntity state Invalid is not valid for email Notifications. Possible states are: [STARTED, SG, WORKFLOW, OUTPUT_INGESTION]."));
//            assertTrue(errorMessages.get(3).contains("partition details can't be null for pipelineStepEntity: pipelineStep1 for Email Notifications."));
//            assertTrue(errorMessages.get(4).contains("pipeline_step name \"pipelineStep2\" mentioned in email notifications doesn't exist for any workflowEntity."));
//        }
//        assertTrue(isException);
//    }
//
//    @Test
//    public void testValidateEmailNotificationDetailsFailureCase2() {
//        inputPartitions.add("partition2");
//        recipients.put("gupta.g@flipkart.com", "");
//        partitionDetailsStep2.put("partition1", "value");
//        partitionDetailsListStep1.add(null);
//        partitionDetailsListStep2.add(partitionDetailsStep2);
//        partitionDetailsMap.put("pipelineStep1", partitionDetailsListStep1);
//        partitionDetailsMap.put("pipelineStep2", partitionDetailsListStep2);
//        pipelineStepNamesToPartitionMapping.put("pipelineStep1", inputPartitions);
//        pipelineStepNamesToPartitionMapping.put("pipelineStep2", inputPartitions);
//        EmailNotifications emailNotifications = EmailNotifications.builder().recipients(recipients)
//                .workflowStates(workflowStates).partitionDetails(partitionDetailsMap).build();
//
//        boolean isException = false;
//        try {
//            EmailNotificationsValidator.validateEmailNotificationDetails(emailNotifications, pipelineStepNamesToPartitionMapping);
//        } catch (Exception e) {
//            isException = true;
//            String message = e.getMessage();
//            List<String> errorMessages = Arrays.asList(message.split("\n"));
//            assertTrue(errorMessages.get(1).contains(" Email Notifications states can't be null for recipient: gupta.g@flipkart.com. Please mention in current format -> [gupta.g@flipkart.com: success, failure/ success/ failure]."));
//            assertTrue(errorMessages.get(2).contains("Partition key value details map of pipeline_step \"pipelineStep1\" are not mentioned for email notifications."));
//            assertTrue(errorMessages.get(3).contains("Partition Keys of pipeline_step \"pipelineStep2\" for email notifications are wrong. Required values: [partition2]"));
//        }
//        assertTrue(isException);
//    }
//
//    @Test
//    public void testValidateEmailNotificationDetailsFailureCase3() {
//        inputPartitions.add("partition1");
//        recipients.put("gupta.g@flipkart.com", "SUCCESSFUL, FAILURE, SUCCESS");
//        partitionDetailsStep2.put("partition1", "value");
//        partitionDetailsListStep1.add(partitionDetailsStep2);
//        partitionDetailsListStep2.add(partitionDetailsStep2);
//        partitionDetailsMap.put("pipelineStep1", partitionDetailsListStep1);
//        partitionDetailsMap.put("pipelineStep2", partitionDetailsListStep2);
//        pipelineStepNamesToPartitionMapping.put("pipelineStep1", inputPartitions);
//        pipelineStepNamesToPartitionMapping.put("pipelineStep2", inputPartitions);
//        EmailNotifications emailNotifications = EmailNotifications.builder().recipients(recipients)
//                .workflowStates(workflowStates).partitionDetails(partitionDetailsMap).build();
//
//        boolean isException = false;
//        try {
//            EmailNotificationsValidator.validateEmailNotificationDetails(emailNotifications, pipelineStepNamesToPartitionMapping);
//        } catch (Exception e) {
//            isException = true;
//            String message = e.getMessage();
//            List<String> errorMessages = Arrays.asList(message.split("\n"));
//            assertTrue(errorMessages.get(1).contains("Email Notifications states can be only success or/and Failure for recipient: gupta.g@flipkart.com."));
//        }
//        assertTrue(isException);
//    }
//
//    @Test
//    public void testValidateEmailNotificationDetailsFailureCase4() {
//        inputPartitions.add("partition1");
//        recipients.put("gupta.g@flipkart.com", "SUCCESSFUL");
//        partitionDetailsStep2.put("partition1", "value");
//        partitionDetailsListStep1.add(partitionDetailsStep2);
//        partitionDetailsListStep2.add(partitionDetailsStep2);
//        partitionDetailsMap.put("pipelineStep1", partitionDetailsListStep1);
//        partitionDetailsMap.put("pipelineStep2", partitionDetailsListStep2);
//        pipelineStepNamesToPartitionMapping.put("pipelineStep1", inputPartitions);
//        pipelineStepNamesToPartitionMapping.put("pipelineStep2", inputPartitions);
//        EmailNotifications emailNotifications = EmailNotifications.builder().recipients(recipients)
//                .workflowStates(workflowStates).partitionDetails(partitionDetailsMap).build();
//
//        boolean isException = false;
//        try {
//            EmailNotificationsValidator.validateEmailNotificationDetails(emailNotifications, pipelineStepNamesToPartitionMapping);
//        } catch (Exception e) {
//            isException = true;
//            String message = e.getMessage();
//            List<String> errorMessages = Arrays.asList(message.split("\n"));
//            assertTrue(errorMessages.get(1).contains("Email Notifications states can be only success or/and failure for recipient: gupta.g@flipkart.com"));
//        }
//        assertTrue(isException);
//    }
//
//    @Test
//    public void testValidateEmailNotificationDetailsFailureSuccess() throws Exception {
//        inputPartitions.add("partition1");
//        recipients.put("gupta.g@flipkart.com", "SUCCESS, FAILURE");
//        partitionDetailsStep2.put("partition1", "value");
//        partitionDetailsListStep1.add(partitionDetailsStep2);
//        partitionDetailsListStep2.add(partitionDetailsStep2);
//        partitionDetailsMap.put("pipelineStep1", partitionDetailsListStep1);
//        partitionDetailsMap.put("pipelineStep2", partitionDetailsListStep2);
//        pipelineStepNamesToPartitionMapping.put("pipelineStep1", inputPartitions);
//        pipelineStepNamesToPartitionMapping.put("pipelineStep2", inputPartitions);
//        EmailNotifications emailNotifications = EmailNotifications.builder().recipients(recipients)
//                .workflowStates(workflowStates).partitionDetails(partitionDetailsMap).build();
//
//        EmailNotificationsValidator.validateEmailNotificationDetails(emailNotifications, pipelineStepNamesToPartitionMapping);
//    }
//
//}
