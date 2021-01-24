package com.flipkart.dsp.azkaban;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.dsp.client.AzkabanClient;
import com.flipkart.dsp.config.AzkabanConfig;
import com.flipkart.dsp.entities.enums.AzkabanConcurrentOption;
import com.flipkart.dsp.entities.enums.AzkabanFailureAction;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.utils.JsonUtils;
import com.flipkart.dsp.utils.NodeMetaData;
import com.ning.http.client.RequestBuilder;

/**
 */
public class AzkabanFlowSubmitRequestV2 extends AbstractAzkabanRequest<AzkabanWorkflowSubmitResponse> {
    private AzkabanConfig azkabanConfig;
    private String sessionId;
    private String jobName;
    private NodeMetaData nodeMetaData;
    private final String azkabanProject;
    private final String disabledNodes;

    public AzkabanFlowSubmitRequestV2(AzkabanClient client, AzkabanConfig azkabanConfig, String sessionId,
                                      String jobName, NodeMetaData nodeMetaData, String azkabanProject, String disabledNodes) {
        super(client);
        this.azkabanConfig = azkabanConfig;
        this.sessionId = sessionId;
        this.jobName = jobName;
        this.nodeMetaData = nodeMetaData;
        this.azkabanProject = azkabanProject;
        this.disabledNodes = disabledNodes;
    }

    @Override
    protected String getMethod() {
        return "GET";
    }

    @Override
    protected String getPath() {
        return "/executor";
    }

    @Override
    protected JavaType getReturnType() {
        return JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(AzkabanWorkflowSubmitResponse.class);
    }

    @Override
    protected RequestBuilder buildRequest(RequestBuilder requestBuilder) {
        try {
            requestBuilder.addHeader("Content-type", "application/x-www-form-urlencoded");

            requestBuilder.addQueryParam("ajax", "executeFlow").
                    addQueryParam("session.id", sessionId).
                    addQueryParam("username", azkabanConfig.getUser()).
                    addQueryParam("password", azkabanConfig.getPassword()).
                    addQueryParam("project", azkabanProject).
                    addQueryParam("flow", jobName).
                    addQueryParam(Constants.AZKABAN_CONCURRENT_OPTION, AzkabanConcurrentOption.QUEUE.getValue()).
                    addQueryParam(Constants.AZKABAN_FAILURE_ACTION, AzkabanFailureAction.FINISH_POSSIBLE.getValue()).
                    addQueryParam("flowOverride[" + Constants.CONFIG_SVC_BUCKETS_KEY + "]", System.getProperty(Constants.CONFIG_SVC_BUCKETS_KEY)).
                    addQueryParam("flowOverride[" + Constants.APPLICATION_CLASS_DYNAMIC_ARGS + "]", objectMapper.writeValueAsString(nodeMetaData));
            if(disabledNodes!=null) {
                requestBuilder.addQueryParam("disabled","["+disabledNodes+"]");
            }
            return requestBuilder;
        } catch (Exception e) {
            throw new RuntimeException("Exception occurred while building request body", e);
        }
    }
}
