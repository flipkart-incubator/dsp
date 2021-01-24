package com.flipkart.dsp.cache;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.File;

@AllArgsConstructor
@Getter
public class ScriptKey {

    private static final String DELIMITER = "__";
    private static final String FILE_DELIMITER = "_";
    private String gitRepo;
    private String gitFolder;
    private String gitCommitId;


    public String getKey() {
        return Joiner.on(DELIMITER)
                .join(Lists.newArrayList(getGitRepo(),getGitFolder(),getGitCommitId()))
                .trim()
                .replace(File.separator,FILE_DELIMITER);
    }
}
