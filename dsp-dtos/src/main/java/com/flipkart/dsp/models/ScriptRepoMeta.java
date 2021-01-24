package com.flipkart.dsp.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class ScriptRepoMeta {
    @JsonProperty("git_commit_id")
    private String gitCommitId;
    @JsonProperty("git_repo")
    private String gitRepo;
}
