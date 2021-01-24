package com.flipkart.dsp.executor.utils;

import com.flipkart.dsp.config.MiscConfig;

import javax.inject.Inject;

public class MesosLocationHelper implements LocationHelper {

    private MiscConfig miscConfig;

    @Inject
    public MesosLocationHelper(MiscConfig miscConfig) {

        this.miscConfig = miscConfig;
    }

    @Override
    public String getLocalPath() {
        return miscConfig.getLocalBasePath();
    }
}
