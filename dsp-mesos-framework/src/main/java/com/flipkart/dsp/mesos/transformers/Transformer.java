package com.flipkart.dsp.mesos.transformers;

import com.flipkart.dsp.mesos.exceptions.TransformationException;

import java.util.ArrayList;
import java.util.List;

abstract public class Transformer<t1,t2> {
    abstract public t2 transform(t1 o1) throws TransformationException;

    public List<t2> transformList(List<t1> o1) throws TransformationException {
        List<t2> o2 = new ArrayList<>();
        for (t1 entry: o1) {
            o2.add(transform(entry));
        }
        return o2;
    }
}
