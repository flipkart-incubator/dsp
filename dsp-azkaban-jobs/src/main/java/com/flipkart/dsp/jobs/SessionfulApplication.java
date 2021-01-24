package com.flipkart.dsp.jobs;

import com.flipkart.dsp.exceptions.AzkabanException;
import com.flipkart.dsp.utils.NodeMetaData;

/**
 */
public interface SessionfulApplication {
    String getName();
    NodeMetaData execute(String[] args) throws AzkabanException;
}
