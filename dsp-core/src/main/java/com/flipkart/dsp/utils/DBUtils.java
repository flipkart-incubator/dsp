package com.flipkart.dsp.utils;

import com.flipkart.dsp.entities.sg.core.BaseEntity;
import org.reflections.Reflections;

import javax.persistence.Entity;
import java.util.Set;

/**
 */

public class DBUtils {
    public static Set<Class<?>> getEntityClasses() {
        Reflections reflections = new Reflections(BaseEntity.class.getPackage().getName());
        return reflections.getTypesAnnotatedWith(Entity.class);
    }
}
