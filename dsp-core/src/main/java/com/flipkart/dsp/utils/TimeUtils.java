package com.flipkart.dsp.utils;

/**
 */
public class TimeUtils {

    public static String convertMillisToHMmSs(long milliSeconds) {
        long ms = milliSeconds % 1000;
        long s = (milliSeconds / 1000) % 60;
        long m = (milliSeconds / (60 * 1000)) % 60;
        long h = (milliSeconds / (60 * 60 * 1000)) ;
        return String.format("%d:%02d:%02d.%03d", h,m,s, ms);
    }
}
