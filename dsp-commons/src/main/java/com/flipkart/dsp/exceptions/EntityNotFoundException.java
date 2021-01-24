package com.flipkart.dsp.exceptions;

import lombok.Getter;

public class EntityNotFoundException extends RuntimeException {

    @Getter
    private String entityName;



    public EntityNotFoundException(String entityName, String message) {
        super(message);
        this.entityName = entityName;
    }

    public EntityNotFoundException(String entityName, String message, Throwable cause) {
        super(message, cause);
        this.entityName = entityName;
    }
}
