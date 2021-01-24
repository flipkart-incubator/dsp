package com.flipkart.dsp.entities.script;

import com.flipkart.dsp.models.ImageLanguageEnum;
import com.flipkart.dsp.models.ScriptVariable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Set;

/**
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ScriptMeta implements Serializable {

    private long id;

    private String gitFilePath;

    private String gitCommitId;

    private String gitFolder;

    private String gitRepo;

    private String execEnv;

    private String imagePath;

    private String startUpScriptPath;

    private  Set<ScriptVariable> inputVariables ;

    private Set<ScriptVariable> outputVariables;


    private ImageLanguageEnum imageLanguageEnum;

}
