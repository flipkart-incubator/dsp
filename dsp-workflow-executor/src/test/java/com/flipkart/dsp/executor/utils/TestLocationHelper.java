package com.flipkart.dsp.executor.utils;

import java.io.File;
import java.nio.file.Paths;

public class TestLocationHelper implements LocationHelper {
    @Override
    public String getLocalPath() {
        return Paths.get(System.getProperty("user.home"), "dsp-tmp").toString().concat(File.separator);
    }
}
