package com.flipkart.dsp.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ExecutionEnvironmentSummary {
    private Long id;
    private String name;
    private Specification specification;

    @JsonProperty("image_path")
    private String imagePath;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Specification {
        private String os;
        @JsonProperty("os_version")
        private String osVersion;

        private ImageLanguageEnum language;
        @JsonProperty("language_version")
        private String languageVersion;

        @JsonProperty("language_libraries")
        private List<LanguageLibraries> languageLibraries;

        @JsonProperty("native_components")
        private List<NativeComponents> nativeComponents;

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class LanguageLibraries {
        private String name;
        private String version;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class NativeComponents {
        private String name;
        private String version;
        private String architecture;
    }
}
