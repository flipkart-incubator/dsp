package com.flipkart.dsp.mesos.exceptions;

public class SchedulerException extends RuntimeException {
    public SchedulerException(String message) {
        super("Scheduler Failed: " + message);
    }

    public SchedulerException(String message, Throwable cause) {
        super("Scheduler Failed: " + message, cause);
    }
}
