package com.flipkart.manager;

import com.flipkart.utils.CompareType;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ComparisonManager {

    public boolean compare(Object expectedValue, Object actualValue, CompareType compareType) {
        boolean isEqual = false;
        log.info("Started compare");
        switch(compareType) {
            case TEXT:
                isEqual = compareText(expectedValue, actualValue);
                break;
                default:
                    log.error("Unable to find compareType " + compareType);
        }
        log.info("Started complete");
        return isEqual;
    }

    private boolean compareText(Object expectedValue, Object actualValue) {
        return expectedValue.toString().trim().equals(actualValue.toString().trim());
    }
}
