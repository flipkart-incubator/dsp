package com.flipkart.dsp.utils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class NodeMetaData {

    @JsonProperty(Constants.PREV_NODE)
    private String prevNode;

    @JsonProperty(Constants.SG_JOB_ID)
    private String sgJobId;

    @JsonProperty("validation_request_id")
    private String validationRequestId;

    @JsonProperty(Constants.DAG_ENTITIES)
    @JsonDeserialize(as=ArrayList.class, contentAs=String.class)
    private List<String> dagEntities;

    @JsonProperty(Constants.REQUEST_ID)
    private Long requestId;

}
