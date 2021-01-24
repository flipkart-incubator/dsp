package com.flipkart.dsp.models;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(value = {ElementType.TYPE_USE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Label {
    String value();
}
