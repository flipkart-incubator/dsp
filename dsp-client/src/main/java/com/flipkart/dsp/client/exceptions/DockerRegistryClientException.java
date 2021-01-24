package com.flipkart.dsp.client.exceptions;

/**
 */
public class DockerRegistryClientException extends Exception {
    public DockerRegistryClientException(String message) {
        super(message);
    }

    public DockerRegistryClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
