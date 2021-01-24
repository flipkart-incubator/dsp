package com.flipkart.dsp.models.workflow;

import com.flipkart.dsp.models.WorkflowGroupCreateDetails;
import com.flipkart.dsp.models.WorkflowGroupExecuteRequest;
import com.flipkart.dsp.models.sg.ConfigurableSGDTO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WorkflowSandboxDetails {
    private WorkflowGroupCreateDetails workflowGroupCreateDetails;
    private WorkflowGroupExecuteRequest workflowGroupExecuteRequest;
    private ConfigurableSGDTO configurableSGDTO;
}
