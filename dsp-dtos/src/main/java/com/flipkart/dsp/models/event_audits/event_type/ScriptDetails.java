package com.flipkart.dsp.models.event_audits.event_type;

import com.flipkart.dsp.models.ScriptRepoMeta;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;


@AllArgsConstructor
@NoArgsConstructor
@Getter
@Builder
public class ScriptDetails {
    private Long pipelineStepId;
    private String fileName;
    private ScriptRepoMeta scriptRepoMeta;
}
