package com.flipkart.dsp.utils;

import com.flipkart.dsp.entities.workflow.Workflow;

import java.util.Comparator;

/**
 * +
 */
public class WorkflowVersionComparator implements Comparator<Workflow> {

        @Override
        public int compare(Workflow workflow1, Workflow workflow2){
            String[] version1Splits = workflow1.getVersion().split("\\.");
            String[] version2Splits = workflow2.getVersion().split("\\.");
            int majorVersionCompare = compareVersion(version1Splits[0], version2Splits[0]);
            if (majorVersionCompare == 0) {
                int minorVersionCompare = compareVersion(version1Splits[1], version2Splits[1]);
                if (minorVersionCompare == 0) {
                    return compareVersion(version1Splits[2], version2Splits[2]); // patch version compare
                }
                return minorVersionCompare;
            }
            return majorVersionCompare;
        }

        private Integer compareVersion(String version1, String version2) {
            return Integer.parseInt(version1) - Integer.parseInt(version2);
        }
    }
