package com.flipkart.dsp.entities.sg.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

@Target(ElementType.TYPE_USE)
public @interface Label {
    String value();
}
