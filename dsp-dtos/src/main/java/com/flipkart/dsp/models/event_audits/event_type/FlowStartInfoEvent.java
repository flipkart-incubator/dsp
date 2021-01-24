package com.flipkart.dsp.models.event_audits.event_type;

import com.flipkart.dsp.models.event_audits.Events;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Builder
public class FlowStartInfoEvent  extends Events implements Serializable {
    private String workflowName;
    private String version;
    private Map<String, ScriptDetails> pipelineStepScriptDetails;
    private String mesosQueue;
    private String azkabanUrl;
    private Long requestId;

    @Override
    public String prettyFormat()
    {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append(workflowName).append(" has started successfully.Following are its details\n");
        stringBuffer.append("Version : ").append(version).append("\n");
        stringBuffer.append("Mesos Queue : ").append(mesosQueue).append("\n");
        stringBuffer.append("Azkaban Url : ").append(azkabanUrl).append("\n");
        stringBuffer.append("Script Details are as Follow : " + "\n");
        stringBuffer.append("RequestID : ").append(requestId).append("\n");

        pipelineStepScriptDetails.forEach((pipelineStepName, scriptDetails) -> {
            stringBuffer.append("Repo Name : ").append(scriptDetails.getScriptRepoMeta().getGitRepo())
                    .append("\nFile Name : ").append(scriptDetails.getFileName())
                    .append("\nCommit Id : ").append(scriptDetails.getScriptRepoMeta().getGitCommitId()).append("\n");
        });
        return stringBuffer.toString();
    }
}
