package com.flipkart.dsp.cosmos;

public class AzkabanCosmosTag {
    public static String azkabanCurrentNodeID;
    public static String workflowName;
    public static long requestId;

    public static void populateValue(String workflow,
                                     long refreshID,
                                     String azkabanNodeID) {
        requestId = refreshID;
        workflowName = workflow;
        azkabanCurrentNodeID = azkabanNodeID;
    }
}
