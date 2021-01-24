package com.flipkart.dsp.entities.script;

import com.flipkart.dsp.models.ImageLanguageEnum;
import com.flipkart.dsp.models.ScriptVariable;
import lombok.*;

import java.io.Serializable;
import java.util.Set;

/**
 */
@Getter
@AllArgsConstructor
@ToString
@NoArgsConstructor
@Setter
@Builder
public class LocalScript implements Serializable{

    private Long id;

    private String filename ;

    private String location;

    private String executionEnvironment;

    private Set<ScriptVariable> inputVariables ;  // Only for setting objects like dataFrame, Model etc.

    private Set<ScriptVariable> outputVariables;

    private ImageLanguageEnum imageLanguageEnum;

}
