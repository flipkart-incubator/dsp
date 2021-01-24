package com.flipkart.dsp.models.externalentities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * +
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = CephEntity.class, name = "CephEntity"),
        @JsonSubTypes.Type(value = FTPEntity.class, name = "FTPEntity")
})
public abstract class ExternalEntity {
}
