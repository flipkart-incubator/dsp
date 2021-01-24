package com.flipkart.dsp.service;

import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.sg.dto.SGJobOutputPayload;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exceptions.DSPCoreException;

public interface WorkflowExecutionService {

    boolean executeWorkflow(Request request, WorkflowDetails workflowDetails, SGJobOutputPayload payload,
                            String workflowExecutionId, String mesosQueue, Boolean failFast, PipelineStep pipelineStep)
            throws DSPCoreException;

    boolean executeSG(Long requestId, WorkflowDetails workflowDetails, PipelineStep pipelineStep,
                      String workflowExecutionId, String mesosQueue, Boolean failFast) throws DSPCoreException;

}
