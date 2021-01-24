package com.flipkart.dsp.utils;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.util.List;

/**
 */
public class StringUtils {

    private static final Joiner JOINER = Joiner.on(",").skipNulls();
    private static final Splitter SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();

    public static String joinList(List<String> list) {
        if (list != null) {
            return JOINER.join(list);
        } else {
            return null;
        }
    }

    public static List<String> splitString(String s) {
        if (s != null) {
            return SPLITTER.splitToList(s);
        } else {
            return null;
        }
    }
}
