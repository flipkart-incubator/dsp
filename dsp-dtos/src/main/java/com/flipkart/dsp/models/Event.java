package com.flipkart.dsp.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
@JsonInclude()
public class Event {

    @JsonProperty
    private Level level;

    @JsonProperty
    private String message;

    @AllArgsConstructor
    public enum Level {
        INFO("info"), DEBUG("debug"), ERROR("error");
        private String value;

        public String getValue() {
            return value;
        }
    }
}

