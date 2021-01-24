package com.flipkart.dsp.health_check;

import com.flipkart.dsp.exceptions.HealthCheckException;

/**
 * +
 */
public interface HealthCheck {
    void check() throws HealthCheckException;
}
