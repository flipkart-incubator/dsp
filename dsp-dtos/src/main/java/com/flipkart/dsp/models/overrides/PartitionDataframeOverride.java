package com.flipkart.dsp.models.overrides;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.HashMap;

@Data
@AllArgsConstructor
public class PartitionDataframeOverride extends HashMap<String/*partitionKey*/, String/*partitionHDFSLocation*/> implements DataframeOverride {

}
