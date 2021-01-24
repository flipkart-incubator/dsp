package com.flipkart.dsp.entities.misc;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.dsp.models.ImageLanguageEnum;
import io.dropwizard.jackson.JsonSnakeCase;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 */
@Data
@Builder
@JsonSnakeCase
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ImageDetail implements Serializable {
    Map<String,String> librarySet;
    ImageLanguageEnum imageLanguage;
    String imageName;
}
