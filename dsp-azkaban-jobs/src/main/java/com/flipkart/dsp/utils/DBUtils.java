package com.flipkart.dsp.utils;

import org.reflections.Reflections;

import javax.persistence.Entity;
import java.util.Set;

/**
 */
/* generic class to inject entity classes using reflection */
public class DBUtils {
    public static Set<Class<?>> getEntityClasses(Class k) {
        Reflections reflections = new Reflections(k.getPackage().getName());
        return reflections.getTypesAnnotatedWith(Entity.class);
    }
}
