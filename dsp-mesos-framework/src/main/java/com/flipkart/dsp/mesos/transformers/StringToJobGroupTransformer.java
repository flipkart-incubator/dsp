package com.flipkart.dsp.mesos.transformers;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.mesos.entities.Job;
import com.flipkart.dsp.mesos.entities.JobGroup;
import com.flipkart.dsp.mesos.exceptions.TransformationException;

import java.io.IOException;
import java.util.List;

public class StringToJobGroupTransformer extends Transformer<String, JobGroup> {
    ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public JobGroup transform(String json) throws TransformationException {
        JobGroup job;
        try {
            job = objectMapper.readValue(json, JobGroup.class);
        } catch (IOException e) {
            throw new TransformationException("",e);
        }
        return job;
    }

    public List<JobGroup> transformList(String json) throws TransformationException {
        List<JobGroup> jobList;
        try {
            jobList = objectMapper.readValue(json,new TypeReference<List<JobGroup>>(){});
        } catch (IOException e) {
            throw new TransformationException("",e);
        }
        return jobList;
    }
}
