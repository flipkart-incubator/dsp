package com.flipkart.dsp.dto.ConfigurableSG;

import lombok.Data;

import java.util.List;
@Data
public class ConfigurableSGPreProcessedDTO {
    List<SGEntity> sgEntityList;
    List<String> errorOutputList;
}
